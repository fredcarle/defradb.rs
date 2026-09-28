//! A filter naming several indexed fields must scan the most selective index.
//!
//! Every document here shares one `market`, whose index is declared first,
//! while each `hour` value matches a handful. Scored by filter shape alone,
//! the two equality conditions tie and the declaration order picks `market`,
//! so the query assembles the whole collection to return ten documents.
//! [`CountingStore`] counts point lookups below the fetcher, which is where
//! document assembly shows up.

use crate::common::counting_store::CountingStore;
use db::AutoCommitMutator;
use db::LensedAutoCommitFetcher;
use db::DB;
use document::Document;
use document::NormalValue;
use query::DocMutator;
use query::QueryExecutor;
use query::QueryRequest;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use schema::IndexDescription;
use schema::IndexedFieldDescription;
use std::sync::Arc;
use storage::RegolithStore;

const COLLECTION_SIZE: usize = 2000;
const HOURS: usize = 200;
const PER_HOUR: usize = COLLECTION_SIZE / HOURS;

fn single_field_index(id: u32, field: &str) -> IndexDescription {
    IndexDescription {
        name: format!("Events_{field}_ASC"),
        id,
        fields: vec![IndexedFieldDescription {
            name: field.to_string(),
            descending: false,
        }],
        unique: false,
        kind: None,
        auto_generated: false,
    }
}

fn schema() -> CollectionVersion {
    let mut version = CollectionVersion::new(
        "Events",
        "events-v1",
        "events-collection",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "market", FieldKind::string()),
            FieldDescription::new("3", "hour", FieldKind::string()),
            FieldDescription::new("4", "seq", FieldKind::int()),
        ],
    );
    version.is_materialized = true;
    // `market` first: on a shape-score tie, declaration order decides.
    version.indexes = vec![
        single_field_index(1, "market"),
        single_field_index(2, "hour"),
    ];
    version
}

async fn seeded_db() -> Arc<DB<CountingStore<RegolithStore>>> {
    let db = Arc::new(DB::new(CountingStore::new(RegolithStore::in_memory().unwrap())).unwrap());
    db.create_collection(schema()).await.unwrap();
    let docs = (0..COLLECTION_SIZE)
        .map(|i| {
            let mut doc = Document::new();
            doc.set("market", NormalValue::String("zcat".to_string()));
            doc.set("hour", NormalValue::String(format!("h{:03}", i % HOURS)));
            // Document IDs are content-addressed; `seq` keeps each one distinct.
            doc.set("seq", NormalValue::Int(i as i64));
            doc
        })
        .collect();
    AutoCommitMutator::new(db.clone())
        .create_many("Events", docs)
        .await
        .unwrap();
    db
}

async fn point_gets_for(db: &Arc<DB<CountingStore<RegolithStore>>>, filter: &str) -> usize {
    let runner = query::QueryRunner::with_provider(
        LensedAutoCommitFetcher::new(db.clone()),
        db::DbCollectionProvider::new_arc(db.clone()),
    );
    let before = db.store().point_gets();
    let response = runner
        .execute(QueryRequest::new(format!(
            "query {{ Events(filter: {filter}) {{ hour }} }}"
        )))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let rows = response.data.as_ref().unwrap()["Events"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        rows.len(),
        PER_HOUR,
        "{filter} must match one hour's documents"
    );
    assert!(rows.iter().all(|row| row["hour"] == "h007"));
    db.store().point_gets() - before
}

#[tokio::test]
async fn equality_on_a_shared_field_does_not_outrank_a_selective_index() {
    let db = seeded_db().await;
    let gets = point_gets_for(&db, r#"{market: {_eq: "zcat"}, hour: {_eq: "h007"}}"#).await;
    assert!(
        gets <= PER_HOUR * 8,
        "a filter matching {PER_HOUR} of {COLLECTION_SIZE} documents performed {gets} point \
         lookups (observed: 63 scanning the hour index, 12003 scanning the market index); the \
         hour index must be scanned, not the market index every document shares"
    );
}

#[tokio::test]
async fn a_range_on_a_selective_field_outranks_equality_on_a_shared_field() {
    let db = seeded_db().await;
    let gets = point_gets_for(
        &db,
        r#"{market: {_eq: "zcat"}, hour: {_ge: "h007", _le: "h007"}}"#,
    )
    .await;
    assert!(
        gets <= PER_HOUR * 8,
        "a range matching {PER_HOUR} of {COLLECTION_SIZE} documents performed {gets} point \
         lookups (observed: 63 scanning the hour index, 12003 scanning the market index); the \
         hour index must be scanned, not the market index every document shares"
    );
}

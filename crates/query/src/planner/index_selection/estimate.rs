//! Entry-count estimates that let index selection prefer selective indexes.
//!
//! The shape score alone cannot tell a condition that matches every document
//! (a field all documents share) from one that matches a handful, so a filter
//! naming both may scan the whole collection. Before planning, each usable
//! index's scan is counted up to [`ESTIMATE_CAP`] entries.

use rapidhash::RapidHashMap;
use schema::CollectionVersion;

use crate::error::Result;
use crate::fetcher::DocFetcher;
use crate::mapper::{Filter, Select};

use super::conditions::can_use_index;
use super::filter_to_scan::filter_to_index_scan;

/// Most index entries counted per candidate; larger scans all tie at the cap.
pub const ESTIMATE_CAP: u64 = 1024;

/// Capped entry counts by index name, for one filter on one collection.
pub type IndexEstimates = RapidHashMap<String, u64>;

/// Count, up to [`ESTIMATE_CAP`], the entries each usable index would scan for `filter`.
///
/// Returns an empty map when fewer than two indexes are usable (there is no
/// choice to inform) or when the fetcher cannot estimate.
pub async fn estimate_filter_indexes(
    fetcher: &dyn DocFetcher,
    collection: &CollectionVersion,
    filter: &Filter,
) -> Result<IndexEstimates> {
    let mut estimates = IndexEstimates::default();
    if !fetcher.supports_index_queries() {
        return Ok(estimates);
    }
    let scans: Vec<_> = collection
        .indexes
        .iter()
        .filter(|index| can_use_index(filter, index))
        .filter_map(|index| filter_to_index_scan(filter, index, None, &collection.fields, None, 0))
        .collect();
    if scans.len() < 2 {
        return Ok(estimates);
    }
    for params in scans {
        match fetcher
            .estimate_index_scan(&collection.name, &params, ESTIMATE_CAP)
            .await?
        {
            Some(count) => {
                estimates.insert(params.index_name, count);
            }
            None => return Ok(IndexEstimates::default()),
        }
    }
    Ok(estimates)
}

/// Estimates for one select's filter, so the planner applies them to that select only.
#[derive(Debug, Clone)]
pub struct SelectEstimates {
    pub collection_name: String,
    pub filter: Filter,
    pub estimates: IndexEstimates,
}

impl SelectEstimates {
    /// The estimates for `select`, if they were computed for its collection and filter.
    pub fn for_select(&self, select: &Select) -> Option<&IndexEstimates> {
        (select.cursor_params.is_none()
            && self.collection_name == select.collection_name
            && select.filter.as_ref() == Some(&self.filter))
        .then_some(&self.estimates)
    }
}

/// Estimate the usable indexes for `select`'s filter.
///
/// Cursor pages skip estimation: a cursor's seek key belongs to the index it
/// was issued on, so the choice must not change between pages.
pub async fn estimate_select(
    fetcher: &dyn DocFetcher,
    collection: &CollectionVersion,
    select: &Select,
) -> Result<Option<SelectEstimates>> {
    let Some(filter) = select.filter.as_ref() else {
        return Ok(None);
    };
    if select.cursor_params.is_some() {
        return Ok(None);
    }
    let estimates = estimate_filter_indexes(fetcher, collection, filter).await?;
    Ok((!estimates.is_empty()).then(|| SelectEstimates {
        collection_name: select.collection_name.clone(),
        filter: filter.clone(),
        estimates,
    }))
}

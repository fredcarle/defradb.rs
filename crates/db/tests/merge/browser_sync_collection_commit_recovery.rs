//! How a `/sync` collection commit survives failure, concurrency, and the trip
//! to a peer.

use std::collections::HashSet;
use std::sync::Arc;

use blockstore::{Blockstore as _, DefraBlockstore};
use cid::Cid;
use db::merge::browser_sync::BrowserSyncEngine;
use db::merge::head_provider::DbHeadProvider;
use db::merge::merge_handler::DbMergeHandler;
use db::AutoCommitMutator;
use db::DB;
use defra_core::block::generate_cid_from_bytes;
use defra_core::browser_sync::{BrowserSyncBlock, BrowserSyncDocument};
use defra_core::merge::{BlockMetadata, MergeHandler, MergeOutcome};
use defra_core::{Block, CrdtDelta, Signature, SignatureHeader, SignatureType};
use document::DocID;
use p2p::sync::DocumentHeadProvider;
use query::mutator::DocMutator;
use storage::RegolithStore;

use crate::browser_sync_collection_commit::{
    authored_elsewhere, branchable_node, collection_heads, command, load_block,
    CapturingBroadcaster, COLLECTION, COLLECTION_ID,
};

/// Every composite root some collection commit reaches, walking the whole
/// collection DAG back from its live heads.
async fn roots_in_collection_dag(db: &Arc<DB<RegolithStore>>) -> HashSet<String> {
    let mut stack = collection_heads(db).await;
    let mut seen = HashSet::new();
    let mut roots = HashSet::new();
    while let Some(cid) = stack.pop() {
        if !seen.insert(cid) {
            continue;
        }
        let block = load_block(db, &cid).await;
        roots.extend(
            block
                .links
                .iter()
                .flatten()
                .map(|link| link.link.to_string()),
        );
        stack.extend(block.heads.iter().flatten().copied());
    }
    roots
}

fn decode(document: &BrowserSyncDocument, cid: &str) -> Block {
    let block = document
        .blocks
        .iter()
        .find(|block| block.cid == cid)
        .expect("wire block is present");
    Block::from_dag_cbor(&hex::decode(&block.data).unwrap()).unwrap()
}

/// The document's genesis composite: the one composite with no parents.
fn genesis_of(document: &BrowserSyncDocument) -> String {
    document
        .blocks
        .iter()
        .find(|wire| {
            let block = decode(document, &wire.cid);
            matches!(block.delta, CrdtDelta::Composite(_))
                && block.heads.as_ref().is_none_or(Vec::is_empty)
        })
        .expect("a document has a genesis composite")
        .cid
        .clone()
}

/// The payload with a second root, `root`, re-signed by a signature that
/// cannot verify. Validation checks only the genesis signature, so this one
/// reaches the merge and is rejected there — after the first root merged.
fn with_a_rejected_later_root(pushed: &BrowserSyncDocument) -> BrowserSyncDocument {
    let mut forged = pushed.clone();
    let genesis = genesis_of(pushed);
    let update = pushed.roots[0].clone();

    let signature = Signature::new(SignatureHeader::new(SignatureType::EdDSA, vec![1]), vec![2]);
    let signature_data = signature.to_dag_cbor().unwrap();
    let signature_cid = generate_cid_from_bytes(&signature_data).unwrap();
    let mut block = decode(pushed, &update);
    block.signature = Some(signature_cid);
    let block_data = block.to_dag_cbor().unwrap();
    let block_cid = generate_cid_from_bytes(&block_data).unwrap();

    let wire = forged
        .blocks
        .iter_mut()
        .find(|wire| wire.cid == update)
        .unwrap();
    wire.cid = block_cid.to_string();
    wire.data = hex::encode(block_data);
    forged.blocks.push(BrowserSyncBlock {
        cid: signature_cid.to_string(),
        data: hex::encode(signature_data),
    });
    forged.roots = vec![genesis, block_cid.to_string()];
    forged
}

#[tokio::test]
async fn a_push_whose_later_root_is_rejected_keeps_the_earlier_commit_and_recovers() {
    let browser = branchable_node().await;
    let central = branchable_node().await;
    let created = AutoCommitMutator::new(browser.clone())
        .create(COLLECTION, command("sensor-7", 1))
        .await
        .unwrap();
    let doc_id = created.doc_id.to_string();
    let mut update = command("sensor-7", 2);
    update.set_id(DocID::from_string(&doc_id).unwrap());
    AutoCommitMutator::new(browser.clone())
        .update(COLLECTION, update, HashSet::from(["seq".to_string()]))
        .await
        .unwrap();
    let pushed = authored_elsewhere(&browser, &doc_id).await;
    let genesis = genesis_of(&pushed);

    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let central_sync = BrowserSyncEngine::with_broadcaster(
        central.clone(),
        Arc::new(CapturingBroadcaster {
            events: events.clone(),
        }),
    );
    central_sync
        .apply_document(&with_a_rejected_later_root(&pushed), "browser")
        .await
        .expect_err("the second root's signature cannot verify");

    // The first root's merge committed, and its commit committed with it.
    let heads = collection_heads(&central).await;
    assert_eq!(heads.len(), 1);
    assert_eq!(
        roots_in_collection_dag(&central).await,
        HashSet::from([genesis.clone()])
    );
    assert!(events.lock().unwrap().is_empty());

    // The honest payload lands the update on top of what the failed push left.
    central_sync
        .apply_document(&pushed, "browser")
        .await
        .unwrap();
    let head = load_block(&central, &collection_heads(&central).await[0]).await;
    assert_eq!(head.heads.clone().unwrap_or_default(), heads);
    assert_eq!(
        roots_in_collection_dag(&central).await,
        HashSet::from([genesis, pushed.roots[0].clone()])
    );
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].collection_block.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_pushes_leave_every_document_in_the_collection_dag() {
    const PUSHES: i64 = 64;

    let browser = branchable_node().await;
    let central = branchable_node().await;
    let mut documents = Vec::new();
    for seq in 0..PUSHES {
        let created = AutoCommitMutator::new(browser.clone())
            .create(COLLECTION, command(&format!("sensor-{seq}"), seq))
            .await
            .unwrap();
        documents.push(authored_elsewhere(&browser, &created.doc_id.to_string()).await);
    }

    let central_sync = Arc::new(BrowserSyncEngine::new(central.clone()));
    let mut pushes = tokio::task::JoinSet::new();
    for document in documents.clone() {
        let central_sync = central_sync.clone();
        pushes.spawn(async move { central_sync.apply_document(&document, "browser").await });
    }
    while let Some(pushed) = pushes.join_next().await {
        pushed.unwrap().unwrap();
    }

    let reachable = roots_in_collection_dag(&central).await;
    for document in &documents {
        assert!(
            reachable.contains(&document.roots[0]),
            "{} is stored but outside the collection DAG",
            document.doc_id
        );
    }
}

/// The commit is unsigned on a server, whose `/sync` handler sets no signing
/// config. A peer must still take it, head and document both, or the commit
/// that makes a document discoverable would be the thing that stops it
/// replicating.
#[tokio::test]
async fn a_peer_merges_the_unsigned_commit_it_was_announced() {
    let browser = branchable_node().await;
    let central = branchable_node().await;
    let peer = branchable_node().await;
    let created = AutoCommitMutator::new(browser.clone())
        .create(COLLECTION, command("sensor-7", 1))
        .await
        .unwrap();
    let doc_id = created.doc_id.to_string();
    let pushed = authored_elsewhere(&browser, &doc_id).await;

    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    BrowserSyncEngine::with_broadcaster(
        central,
        Arc::new(CapturingBroadcaster {
            events: events.clone(),
        }),
    )
    .apply_document(&pushed, "browser")
    .await
    .unwrap();
    let (commit_cid, commit_block) = events.lock().unwrap()[0]
        .collection_block
        .clone()
        .expect("the commit is announced");
    assert_eq!(Block::from_dag_cbor(&commit_block).unwrap().signature, None);

    let blockstore = Arc::new(DefraBlockstore::new(peer.store().clone(), true));
    for wire in &pushed.blocks {
        blockstore
            .put(
                &Cid::try_from(wire.cid.as_str()).unwrap(),
                &hex::decode(&wire.data).unwrap(),
            )
            .await
            .unwrap();
    }
    blockstore.put(&commit_cid, &commit_block).await.unwrap();

    let outcome = DbMergeHandler::new(peer.clone(), blockstore)
        .handle_block(
            &commit_cid,
            &commit_block,
            BlockMetadata::normal("", COLLECTION_ID, "central", Some("central"), false),
        )
        .await
        .unwrap();

    assert!(matches!(outcome, MergeOutcome::Merged));
    assert_eq!(collection_heads(&peer).await, vec![commit_cid]);
    assert!(!DbHeadProvider::new(peer)
        .get_document_heads(&doc_id)
        .await
        .unwrap()
        .is_empty());
}

/// A browser that created and then updated a document before it synced pushes
/// both revisions as one payload whose only root is the update. The merge walks
/// the unmerged genesis first, and the commit belongs to the root alone: one
/// commit per push root, linking the root the push named.
#[tokio::test]
async fn a_push_carrying_unmerged_history_commits_once_for_its_root() {
    let browser = branchable_node().await;
    let central = branchable_node().await;
    let created = AutoCommitMutator::new(browser.clone())
        .create(COLLECTION, command("sensor-7", 1))
        .await
        .unwrap();
    let doc_id = created.doc_id.to_string();
    let mut update = command("sensor-7", 2);
    update.set_id(DocID::from_string(&doc_id).unwrap());
    AutoCommitMutator::new(browser.clone())
        .update(COLLECTION, update, HashSet::from(["seq".to_string()]))
        .await
        .unwrap();
    let pushed = authored_elsewhere(&browser, &doc_id).await;
    assert_eq!(pushed.roots.len(), 1);

    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    BrowserSyncEngine::with_broadcaster(
        central.clone(),
        Arc::new(CapturingBroadcaster {
            events: events.clone(),
        }),
    )
    .apply_document(&pushed, "browser")
    .await
    .unwrap();

    let heads = collection_heads(&central).await;
    assert_eq!(heads.len(), 1);
    let head = load_block(&central, &heads[0]).await;
    assert_eq!(head.heads.clone().unwrap_or_default(), Vec::<Cid>::new());
    assert_eq!(
        roots_in_collection_dag(&central).await,
        HashSet::from([pushed.roots[0].clone()]),
        "the push named one root, so the collection gains one commit, for it"
    );
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].collection_block.as_ref().map(|(cid, _)| *cid),
        Some(heads[0]),
        "the announcement carries the root's commit, not an ancestor's"
    );
}

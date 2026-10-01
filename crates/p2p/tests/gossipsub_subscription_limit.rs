#![cfg(feature = "test-utils")]

//! A peer's document-topic subscriptions must reach us however many there are.
//!
//! Every document is its own gossipsub topic, so a node holding more than a
//! hundred documents subscribes to more than a hundred topics. go-libp2p-pubsub
//! accepts any number; rust-libp2p gossipsub 0.50 defaults to a filter that
//! drops a peer's whole subscription RPC past 100 topics, which would leave us
//! blind to that peer on every topic.

use std::time::Duration;

use p2p::testutil::MockBitswapStore;
use p2p::{DefraTopic, P2PHost, PeerId};

const DOC_TOPICS: usize = 150;

async fn wait_until_connected(handle: &p2p::P2PHostHandle, peer_id: PeerId) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !handle
        .connected_peers()
        .await
        .unwrap_or_default()
        .contains(&peer_id)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for connection to {peer_id}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn peer_with_more_than_a_hundred_doc_topics_is_seen_on_each() {
    let (host0, handle0, _events0, _r0) = P2PHost::new(MockBitswapStore::new()).await.unwrap();
    let (host1, handle1, _events1, _r1) = P2PHost::new(MockBitswapStore::new()).await.unwrap();
    tokio::spawn(host0.run());
    tokio::spawn(host1.run());

    let doc_ids: Vec<String> = (0..DOC_TOPICS)
        .map(|i| format!("bae-subscription-limit-{i}"))
        .collect();
    for id in &doc_ids {
        handle1
            .subscribe(DefraTopic::Document(id.clone()))
            .await
            .unwrap();
    }
    let shared = doc_ids.last().unwrap().clone();
    handle0
        .subscribe(DefraTopic::Document(shared.clone()))
        .await
        .unwrap();

    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let addr1 = handle1.listen_addresses().await.unwrap().remove(0);
    let peer1 = handle1.local_peer_id_cached();
    handle0.dial(peer1, vec![addr1]).await.unwrap();
    wait_until_connected(&handle0, peer1).await;

    // `topic_peers` reports every connected peer, so only a publish shows
    // whether gossipsub itself knows peer1 is subscribed: with no known
    // subscriber it fails with InsufficientPeers.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let result = handle0.publish_raw(shared.clone(), b"probe".to_vec()).await;
        if result.is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "peer0 never learned peer1's subscriptions across {DOC_TOPICS} doc topics: {result:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
}

//! `CeleriantPool::tail_shard` pages every shard's WAL and long-polls for new writes.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_msg::request::requests::TailShardRequest;
use celeriant_msg::response::responses::TailEntryKind;
use celeriant_wal::aggregate_key::AggregateKey;
use crate::{ServerConfig, TestServer};

use crate::common::{event, port_for, unique_key, R};

const TYPE: u64 = 100;
const SHARDS: u64 = 3;

fn tail(shard_id: u64, after_wal_seq: u64, wait_ms: u32) -> TailShardRequest {
    TailShardRequest { correlation_id: None, shard_id, after_wal_seq, max_bytes: 4096, wait_ms, orgs: None }
}

/// Tail one shard from `after` until a response comes back empty, returning the
/// (key, version) pairs seen and the cursor to resume from.
async fn drain_shard(
    pool: &CeleriantPool,
    shard_id: u64,
    mut after: u64,
    seen: &mut HashSet<(AggregateKey, u64)>,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut last_entry_seq = 0u64;
    loop {
        let resp = pool.tail_shard(tail(shard_id, after, 0)).await?;
        for entry in &resp.entries {
            if entry.wal_seq <= last_entry_seq {
                return Err(format!("shard {shard_id}: wal_seq {} not above {last_entry_seq}", entry.wal_seq).into());
            }
            last_entry_seq = entry.wal_seq;
            if let TailEntryKind::Events { aggregate_key, batch } = &entry.kind {
                seen.insert((aggregate_key.clone(), batch.aggregate_version));
            }
        }
        if resp.read_wal_seq < after {
            return Err(format!("shard {shard_id}: read_wal_seq {} went backwards from {after}", resp.read_wal_seq).into());
        }
        if resp.entries.is_empty() {
            return Ok(resp.read_wal_seq.max(after));
        }
        after = resp.read_wal_seq;
    }
}

pub async fn tail_shard_end_to_end() -> R {
    let config = ServerConfig { num_shards: Some(SHARDS as usize), standalone: true, ..Default::default() };
    let server = TestServer::start_with_config(port_for("tail_shard_end_to_end"), config).await?;
    let pool = CeleriantPool::new(PoolOptions::new(server.address()));

    let keys: Vec<AggregateKey> = (0..3).map(|i| unique_key(&format!("tail_shard_end_to_end_{i}"))).collect();
    let mut written: HashSet<(AggregateKey, u64)> = HashSet::new();
    let mut versions = [0u64; 3];
    for i in 0..10usize {
        let a = i % 3;
        pool.write_events(keys[a].clone(), vec![event(i as u64 + 1, TYPE, 1000 + i as u64, "payload")], 0).await?;
        versions[a] += 1;
        written.insert((keys[a].clone(), versions[a]));
    }

    let mut seen: HashSet<(AggregateKey, u64)> = HashSet::new();
    let mut cursors = Vec::new();
    for shard in 0..SHARDS {
        cursors.push(drain_shard(&pool, shard, 0, &mut seen).await?);
    }
    if seen != written {
        return Err(format!("tailed {} batches, wrote {}: {seen:?} vs {written:?}", seen.len(), written.len()).into());
    }

    // An 11th write lands while a long-poll is waiting on every shard.
    let late_key = keys[0].clone();
    let polls = futures::future::join_all((0..SHARDS).map(|s| {
        let pool = &pool;
        let after = cursors[s as usize];
        async move {
            let started = Instant::now();
            let resp = pool.tail_shard(tail(s, after, 2000)).await;
            (started.elapsed(), resp)
        }
    }));
    let writer = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        pool.write_events(late_key.clone(), vec![event(11, TYPE, 2000, "late")], 0).await
    };
    let (results, write) = tokio::join!(polls, writer);
    write?;
    let late_version = versions[0] + 1;
    let mut found_after = None;
    for (elapsed, resp) in results {
        for entry in resp?.entries {
            if let TailEntryKind::Events { aggregate_key, batch } = entry.kind {
                if aggregate_key == late_key && batch.aggregate_version == late_version {
                    found_after = Some(elapsed);
                }
            }
        }
    }
    let elapsed = found_after.ok_or("long-poll did not return the 11th write")?;
    if elapsed < Duration::from_millis(250) || elapsed >= Duration::from_millis(1500) {
        return Err(format!("long-poll returned after {elapsed:?}, expected shortly after the write").into());
    }
    Ok(())
}

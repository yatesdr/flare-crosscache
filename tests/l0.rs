mod support;
use flare_crosscache::{
    arena::Arena,
    crosscache, disk,
    record::{self, Header},
    store::Config,
    tree::{Eviction, Tree},
};
use std::{
    fs,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use support::*;

const CHILD: &str = "CM_CHILD_BASE";
const CHILD_POINT: &str = "CM_CHILD_POINT";
const CHILD_DUMP: &str = "CM_CHILD_DUMP";
const CHILD_QUARANTINE: &str = "CM_CHILD_QUARANTINE";

#[test]
fn l0_01_tree_insert_chain_remove_parity() {
    for pt in [64, 128, 256] {
        let mut tree = Tree::new(pt);
        let keys = [key(700), key(1300), key(1990)];
        for (index, key) in keys.iter().enumerate() {
            let parent = index.checked_sub(1).map(|index| keys[index]);
            let pages =
                key.consumed_tokens / pt - parent.map_or(0, |parent| parent.consumed_tokens / pt);
            tree.insert(*key, parent, [4; 16], pages as usize, true, ())
                .unwrap();
            tree.access_at(key, index as u64 + 1);
        }
        let parent = tree.node(&keys[2]).unwrap().parent.unwrap();
        assert_eq!(parent, keys[1]);
        assert_eq!(tree.node(&parent).unwrap().parent, Some(keys[0]));
        let available =
            tree.node(&parent).unwrap().tail_pages + tree.node(&keys[2]).unwrap().tail_pages;
        let skip = (1000 / pt - 700 / pt) as usize;
        assert_eq!(available - skip, (1990 / pt - 1000 / pt) as usize);
        assert!(
            tree.insert(key(2000), Some(key(1999)), [0; 16], 0, true, ())
                .is_err()
        );
        assert!(
            tree.insert(key(2000), Some(keys[2]), [0; 16], 20, true, ())
                .is_err()
        );
        assert_eq!(tree.node(&keys[2]).unwrap().children(), 0);
        assert!(tree.remove(&keys[0]).is_err());
        tree.pin(&keys[2]).unwrap();
        assert!(tree.remove(&keys[2]).is_err());
        tree.unpin(&keys[2]).unwrap();
        assert_eq!(tree.remove(&keys[2]).unwrap()[0].key, keys[2]);
        assert_eq!(tree.node(&keys[1]).unwrap().children(), 0);
    }
    let fixture = include_bytes!("fixtures/flare-header-v1.bin");
    let decoded = Header::decode(fixture).unwrap();
    let mut planned = Header::new(&descriptor(), key(130), Some(key(65)), [4; 16], 1).unwrap();
    planned.created = 1_700_000_000;
    planned.row_checksum = 0x0123456789abcdef;
    assert_eq!(decoded, planned);
    assert_eq!(&encode_header(&planned), fixture);
    for (offset, value) in [(129, 0u64), (173, 0), (181, u64::MAX)] {
        let mut invalid = *fixture;
        invalid[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        assert!(Header::decode(&invalid).is_err());
    }
    let arena = Arena::new(1024 * 1024).unwrap();
    let extent = Arc::new(arena.allocate(9000).unwrap());
    assert_eq!(extent.bytes().as_ptr() as usize % 4096, 0);
    let reader = extent.clone();
    drop(extent);
    assert_eq!(arena.used(), 16384);
    drop(reader);
    assert_eq!(arena.used(), 0);
    assert!(arena.allocate(1024 * 1024).is_some());
}

#[test]
fn l0_02_leaf_lru_compaction_and_cascade() {
    let mut tree = Tree::new(64);
    for (tokens, parent) in [(200, None), (400, Some(200)), (600, Some(400))] {
        tree.insert(
            key(tokens),
            parent.map(key),
            [0; 16],
            (tokens / 64 - parent.unwrap_or(0) / 64) as usize,
            true,
            (),
        )
        .unwrap();
        tree.access_at(&key(tokens), tokens);
    }
    tree.access_at(&key(200), 1000);
    tree.pin(&key(600)).unwrap();
    assert!(tree.evict(false, |_| true).is_none());
    assert!(matches!(tree.evict(true, |_| true), Some(Eviction::Compacted(k)) if k == key(400)));
    assert!(!tree.node(&key(400)).unwrap().row);
    assert!(tree.node(&key(600)).unwrap().row);
    tree.unpin(&key(600)).unwrap();
    let Some(Eviction::Leaves(removed)) = tree.evict(false, |_| true) else {
        panic!("leaves")
    };
    assert_eq!(
        removed.iter().map(|node| node.key).collect::<Vec<_>>(),
        [key(600), key(400)]
    );
    assert_eq!(tree.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_03_accept_shapes_and_present_inflight() {
    let mut host = Host::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    host.start();
    let mut a = host.client().await;
    let mut b = host.client().await;
    // A child shaped against a readable parent gets first_page = parent pages.
    let (sender, replies) = a.raw_begin(key(128), None).await;
    // A second connection sees INFLIGHT while the first Put is open.
    assert_eq!(
        b.put(key(128), None, PutOptions::default()).await,
        PutOutcome::Refused(crosscache::Reason::Inflight as u8)
    );
    sender
        .send(crosscache::PutMessage {
            m: Some(crosscache::put_message::M::Cancel(crosscache::Cancel {})),
        })
        .await
        .unwrap();
    drop(sender);
    drop(replies);
    host.wait(|host| host.gauge("open_ops") == 0);
    let bytes = b.store(key(128), None).await;
    assert!(!bytes.is_empty());
    assert_eq!(
        b.put(key(128), None, PutOptions::default()).await,
        PutOutcome::Refused(crosscache::Reason::Present as u8)
    );
    let mut c = host.client().await;
    let mut open = Vec::new();
    for tokens in [640, 768, 896, 1024] {
        open.push(c.raw_begin(key(tokens), None).await);
    }
    assert_eq!(
        c.put(key(1152), None, PutOptions::default()).await,
        PutOutcome::Refused(crosscache::Reason::Queue as u8)
    );
    drop(open);
    host.wait(|host| host.gauge("open_ops") == 0);
    b.store(key(256), Some(key(128))).await;
    b.store(key(512), None).await;
    let mut fresh = host.client().await;
    let mut leaves = Vec::new();
    for (index, tokens) in [3u64, 5].into_iter().enumerate() {
        let bytes = fresh.store(key(tokens), (index > 0).then(|| key(3))).await;
        assert!(!bytes.is_empty());
        leaves.push(tokens);
    }
    let data = fresh.fetch(&[key(3), key(5)], 0, 0).await;
    assert_eq!(data.len(), 2);
    assert!(!data[0].is_empty() && !data[1].is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_04_invalid_checksum_short_payload_and_offsets() {
    let mut host = Host::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    host.start();
    for (options, tokens) in [
        (
            PutOptions {
                corrupt_page: Some(0),
                ..PutOptions::default()
            },
            128,
        ),
        (
            PutOptions {
                early_commit: true,
                ..PutOptions::default()
            },
            128 * 64,
        ),
        (
            PutOptions {
                corrupt_row: true,
                ..PutOptions::default()
            },
            256,
        ),
        (
            PutOptions {
                skip_filled: true,
                ..PutOptions::default()
            },
            192 * 64,
        ),
    ] {
        let mut client = host.client().await;
        let outcome = client.put(key(tokens), None, options).await;
        assert!(
            matches!(outcome, PutOutcome::Refused(reason) if reason == crosscache::Reason::Invalid as u8),
            "{tokens}: {outcome:?}"
        );
    }
    host.wait(|host| host.gauge("nvme_reserved_bytes") == 0);
    assert_eq!(host.gauge("staging_allocated_bytes"), 0);
    let mut check = host.client().await;
    let subscription = check.subscribe().await;
    assert!(subscription.entries.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_05_parent_first_chain_closed_durability() {
    let base = base("durable");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4 * 1024 * 1024,
        4 * 1024 * 1024,
    );
    config.name = instance_name("durable");
    config.faults.pause_at = Some("parent_write".into());
    let signal = base.join("gate");
    config.faults.signal = Some(signal.clone());
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    wait(|| signal.exists());
    client.store(key(256), Some(key(128))).await;
    let namespace = client.descriptor.name();
    assert!(
        !host
            .config
            .root
            .as_ref()
            .unwrap()
            .join(&namespace)
            .join(record::file_name(&key(256)))
            .exists()
    );
    assert_eq!(host.gauge("nvme_bytes_total"), 0);
    fs::remove_file(&signal).unwrap();
    wait(|| signal.exists());
    assert!(
        host.config
            .root
            .as_ref()
            .unwrap()
            .join(&namespace)
            .join(record::file_name(&key(128)))
            .exists()
    );
    fs::remove_file(signal).unwrap();
    host.durable(2);
    let subscription = client.subscribe().await;
    assert!(
        subscription.entries.values().all(|entry| entry.durable),
        "both records must be chain-durable"
    );
}

#[test]
fn l0_06_multiprocess_kill_recovery() {
    if let Ok(base) = std::env::var(CHILD) {
        let base = PathBuf::from(base);
        let point = std::env::var(CHILD_POINT).unwrap_or_default();
        let memory = if point == "parent_write" {
            4 * 1024 * 1024
        } else {
            4096
        };
        let mut config = Config::new(
            base.join("records"),
            base.join("run/m.sock"),
            memory,
            64 * 1024 * 1024,
        );
        config.name = instance_name("child");
        config.faults.pause_at = Some(point.clone());
        config.faults.signal = Some(base.join("gate"));
        if let Ok(dump) = std::env::var(CHILD_DUMP) {
            let store = flare_crosscache::store::Store::open(config).unwrap();
            let snapshot = store
                .namespace_id()
                .and_then(|id| store.snapshot(id))
                .map(|(_, entries)| entries.len())
                .unwrap_or(0);
            fs::write(dump, snapshot.to_string()).unwrap();
            loop {
                std::thread::park();
            }
        }
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut host = Host::open_config(config);
            host.start();
            let mut client = host.client().await;
            if point == "data" {
                let _ = client
                    .put(
                        key(128),
                        None,
                        PutOptions {
                            cancel: Some(CancelAt::Filled),
                            ..PutOptions::default()
                        },
                    )
                    .await;
            } else {
                client.store(key(128), None).await;
            }
            loop {
                std::thread::park();
            }
        });
    }
    for point in ["data", "before_rename", "after_rename", "parent_write"] {
        let base = base(point);
        let config = Config::new(
            base.join("records"),
            base.join("run/m.sock"),
            4096,
            64 * 1024 * 1024,
        );
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "l0_06_multiprocess_kill_recovery", "--nocapture"])
            .env(CHILD, &base)
            .env(CHILD_POINT, point)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        wait(|| base.join("gate").exists());
        child.kill().unwrap();
        let status = child.wait().unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        let lock = disk::RootLock::acquire(&config.root.as_ref().unwrap().clone()).unwrap();
        let recovered = recovered_keys(config.root.as_ref().unwrap());
        assert_eq!(
            recovered,
            if point == "after_rename" {
                vec![key(128)]
            } else {
                vec![]
            },
            "{point}"
        );
        assert!(
            fs::read_dir(config.root.as_ref().unwrap().join(descriptor().name()))
                .unwrap()
                .all(|entry| !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp"))
        );
        drop(lock);
        let _ = fs::remove_file(base.join("gate"));
        let dump = base.join("dump");
        let mut restarted = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "l0_06_multiprocess_kill_recovery", "--nocapture"])
            .env(CHILD, &base)
            .env(CHILD_DUMP, &dump)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        wait(|| dump.exists());
        assert_eq!(
            fs::read_to_string(&dump).unwrap().parse::<usize>().unwrap(),
            recovered.len()
        );
        restarted.kill().unwrap();
        restarted.wait().unwrap();
        fs::remove_dir_all(base).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_07_streaming_and_nvme_oversize() {
    let mut host = Host::new(65536, 2 * 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let bytes = client.store(key(128 * 64), None).await;
    host.durable(1);
    assert_eq!(host.gauge("memory_allocated_bytes"), 0);
    let data = client.fetch(&[key(128 * 64)], 0, 128).await;
    assert_eq!(data[0], bytes);
    assert_eq!(
        client
            .put(key(1024 * 64), None, PutOptions::default())
            .await,
        PutOutcome::Refused(crosscache::Reason::Oversize as u8)
    );
    host.stop();
    host.start();
    let mut client = host.client().await;
    let subscription = client.subscribe().await;
    assert_eq!(subscription.entries.len(), 1);
    assert_eq!(
        client.fetch(&[key(128 * 64)], 64, 64).await[0].len(),
        bytes.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_08_refill_and_split_units() {
    let mut refill = Host::new(65536, 1024 * 1024);
    refill.start();
    let mut client = refill.client().await;
    client.store(key(128), None).await;
    refill.durable(1);
    refill.stop();
    refill.start();
    let mut client = refill.client().await;
    let subscription = client.subscribe().await;
    assert_eq!(
        subscription.entries[&key(128)].tier,
        crosscache::Tier::Nvme as i32
    );
    client.fetch(&[key(128)], 0, 2).await;
    refill.wait(|host| host.gauge("memory_allocated_bytes") > 0);
    let subscription = client.subscribe().await;
    assert_eq!(
        subscription.entries[&key(128)].tier,
        crosscache::Tier::Dram as i32
    );

    let mut split = Host::new(4096, 32 * 1024 * 1024);
    split.start();
    let mut geometry = descriptor();
    geometry.ranks[0].planes = vec![4 * 1024 * 1024 + 4096];
    let mut client = split.client_with(geometry).await.unwrap();
    let expected = client.store(key(128), None).await;
    let actual = client.fetch(&[key(128)], 0, 2).await;
    assert_eq!(actual[0], expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_09_disk_failure_is_refused_io() {
    let base = base("full");
    fs::create_dir_all(&base).unwrap();
    let full = base.join("full");
    fs::write(&full, b"full").unwrap();
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("full");
    config.faults.disk_full = Some(full.clone());
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    // The record does not fit the tiny arena, so it streams through staging.
    assert_eq!(
        client.put(key(512 * 64), None, PutOptions::default()).await,
        PutOutcome::Refused(crosscache::Reason::Io as u8)
    );
    host.wait(|host| host.gauge("nvme_errors_total") > 0);
    // A single-window stream must also get its IO refusal: the failing chunk is
    // the last pending user, so removing the operation first would lose it.
    let mut single = host.client().await;
    assert_eq!(
        single.put(key(64 * 64), None, PutOptions::default()).await,
        PutOutcome::Refused(crosscache::Reason::Io as u8)
    );
    fs::remove_file(full).unwrap();
    let bytes = client.store(key(128), None).await;
    assert!(!bytes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_10_cancel_drains_and_staging_gauge() {
    let base = base("cancel");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        4 * 1024 * 1024,
    );
    config.name = instance_name("cancel");
    config.faults.delay_ms = 300;
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    // Streaming Put: two pages plus the row, one window of 12288 bytes.
    assert_eq!(
        client
            .put(
                key(128),
                None,
                PutOptions {
                    cancel: Some(CancelAt::Filled),
                    ..PutOptions::default()
                },
            )
            .await,
        PutOutcome::Cancelled
    );
    // The staging window lives until its delayed disk write completes, then drains.
    host.wait(|host| host.gauge("staging_allocated_bytes") > 0);
    assert_eq!(host.gauge("staging_allocated_bytes"), 16384);
    host.wait(|host| host.gauge("staging_allocated_bytes") == 0);
    host.wait(|host| host.gauge("nvme_reserved_bytes") == 0);
    // A Get is not blocked by a draining Put.
    let stored = client.store(key(128 * 64), None).await;
    let mut getter = host.client().await;
    let start = Instant::now();
    let data = getter.fetch(&[key(128 * 64)], 0, 128).await;
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(data[0], stored);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_11_snapshot_deltas_epochs_and_namespaces() {
    let mut host = Host::new(1024 * 1024, 4 * 1024 * 1024);
    host.start();
    let mut a = host.client().await;
    a.store(key(128), None).await;
    a.store(key(256), Some(key(128))).await;
    a.store(key(384), Some(key(256))).await;
    let mut subscription = a.subscribe().await;
    assert_eq!(subscription.snapshots.last().unwrap().len(), 3);
    a.store(key(512), Some(key(384))).await;
    subscription.until_added(key(512)).await;
    assert_eq!(subscription.entries.len(), 4);

    let mut other = descriptor();
    other.fingerprint = [9; 16];
    let mut other_client = host.client_with(other).await.unwrap();
    let other_subscription = other_client.subscribe().await;
    assert!(other_subscription.entries.is_empty());

    let mut wrong = descriptor();
    wrong.ranks[0].row_bytes += 1;
    assert!(host.client_with(wrong).await.is_err());

    // A second Subscribe on the same connection replaces the first.
    let mut first = a.subscribe().await;
    let mut second = a.subscribe().await;
    assert!(first.stream.message().await.unwrap().is_none());
    a.store(key(640), Some(key(512))).await;
    second.until_added(key(640)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_12_round_robin_and_dead_client_cleanup() {
    let mut host = Host::new(8 * 1024 * 1024, 64 * 1024 * 1024);
    host.start();
    let mut a = host.client().await;
    let mut client = host.client().await;
    let mut previous = None;
    for index in 1..=160u64 {
        let value = key(index * 64);
        client.store(value, previous).await;
        previous = Some(value);
    }
    host.durable(160);
    let keys: Vec<_> = (1..=160).map(|index| key(index * 64)).collect();
    let mut dead = host.client().await;
    for _ in 0..4 {
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        let _replies = dead
            .inner
            .get(dead.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
            .await
            .unwrap();
        sender
            .send(crosscache::GetMessage {
                m: Some(crosscache::get_message::M::Begin(crosscache::GetBegin {
                    namespace_id: dead.descriptor.namespace_id().to_vec(),
                    chain: keys.iter().map(|key| key_msg(*key)).collect(),
                    skip_pages: 0,
                    expected_pages: 160,
                    row: true,
                    destinations: Vec::new(),
                    transfer: 0,
                })),
            })
            .await
            .unwrap();
        drop(sender);
    }
    drop(dead);
    let start = Instant::now();
    a.fetch(&[key(64)], 0, 1).await;
    assert!(start.elapsed() < Duration::from_millis(500));
    host.wait(|host| host.gauge("leases") == 0 && host.gauge("cleanup_records") == 0);
    // Unknown transfers are dropped without disturbing a live connection.
    let mut unknown = host.client().await;
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = unknown
        .inner
        .get(unknown.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(crosscache::GetMessage {
            m: Some(crosscache::get_message::M::Begin(crosscache::GetBegin {
                namespace_id: unknown.descriptor.namespace_id().to_vec(),
                chain: vec![key_msg(key(64))],
                skip_pages: 0,
                expected_pages: 1,
                row: true,
                destinations: Vec::new(),
                transfer: 0,
            })),
        })
        .await
        .unwrap();
    let _leased = loop {
        let reply = replies.message().await.unwrap().unwrap();
        if matches!(reply.r, Some(crosscache::get_reply::R::Leased(_))) {
            break reply;
        }
    };
    sender
        .send(crosscache::GetMessage {
            m: Some(crosscache::get_message::M::Released(
                crosscache::WindowReleased {
                    transfer: 999_999,
                    index: 0,
                },
            )),
        })
        .await
        .unwrap();
    let refused = loop {
        let reply = replies.message().await.unwrap().unwrap();
        if matches!(reply.r, Some(crosscache::get_reply::R::Refused(_))) {
            break reply;
        }
    };
    assert!(matches!(
        refused.r,
        Some(crosscache::get_reply::R::Refused(_))
    ));
    let start = Instant::now();
    a.fetch(&[key(64)], 0, 1).await;
    assert!(start.elapsed() < Duration::from_millis(500));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_13_generation_fencing_and_malformed_streams() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    let old = client.generation;
    // A second Hello on the same transport supersedes the previous generation.
    let hello = client
        .inner
        .hello(tonic::Request::new(crosscache::HelloRequest {
            protocol: 1,
            replica: "again".into(),
            descriptor: Some(descriptor_msg(&client.descriptor)),
        }))
        .await
        .unwrap()
        .into_inner();
    let mut request = tonic::Request::new(crosscache::SubscribeRequest {
        namespace_id: client.descriptor.namespace_id().to_vec(),
        generation: old,
        descriptor: Some(descriptor_msg(&client.descriptor)),
    });
    request
        .metadata_mut()
        .insert("x-crosscache-generation", old.to_string().parse().unwrap());
    let status = client.inner.subscribe(request).await.err().unwrap();
    assert_eq!(status.code(), tonic::Code::Aborted);
    client.generation = hello.generation;

    // Missing metadata is UNAUTHENTICATED.
    let request = tonic::Request::new(crosscache::SubscribeRequest {
        namespace_id: client.descriptor.namespace_id().to_vec(),
        generation: client.generation,
        descriptor: Some(descriptor_msg(&client.descriptor)),
    });
    let status = client.inner.subscribe(request).await.err().unwrap();
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    // A filled window for an unknown transfer is refused INVALID.
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(crosscache::PutMessage {
            m: Some(client.begin(key(256), None)),
        })
        .await
        .unwrap();
    let _accept = replies.message().await.unwrap().unwrap();
    sender
        .send(crosscache::PutMessage {
            m: Some(crosscache::put_message::M::Filled(
                crosscache::WindowFilled {
                    transfer: 999_999,
                    index: 0,
                    page_xxh3: vec![0],
                    row_xxh3: 0,
                    has_row: false,
                },
            )),
        })
        .await
        .unwrap();
    let refused = loop {
        match replies.message().await.unwrap().unwrap().r {
            Some(crosscache::put_reply::R::Refused(refused)) => break refused,
            Some(_) => continue,
            None => panic!("ended"),
        }
    };
    assert_eq!(refused.reason, crosscache::Reason::Invalid as i32);

    // A Commit before Begin is a malformed stream.
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(crosscache::PutMessage {
            m: Some(crosscache::put_message::M::Commit(crosscache::PutCommit {
                transfer: 1,
                metadata_offset: 0,
                metadata: vec![0; 4096],
            })),
        })
        .await
        .ok();
    let refused = replies.message().await.unwrap().unwrap();
    assert!(matches!(
        refused.r,
        Some(crosscache::put_reply::R::Refused(crosscache::Refused {
            reason,
            ..
        })) if reason == crosscache::Reason::Invalid as i32
    ));
    drop(sender);
    drop(replies);

    // An empty Put stream is a terminal Refused{INVALID}, not a status.
    let (empty_sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    drop(empty_sender);
    let refused = replies.message().await.unwrap().unwrap();
    assert!(matches!(
        refused.r,
        Some(crosscache::put_reply::R::Refused(crosscache::Refused {
            reason,
            ..
        })) if reason == crosscache::Reason::Invalid as i32
    ));

    // LocalCache.Get must reject destinations as a terminal refusal too.
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .get(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(crosscache::GetMessage {
            m: Some(crosscache::get_message::M::Begin(crosscache::GetBegin {
                namespace_id: client.descriptor.namespace_id().to_vec(),
                chain: vec![key_msg(key(128))],
                skip_pages: 0,
                expected_pages: 2,
                row: true,
                destinations: vec![crosscache::GetDestination {
                    record: 0,
                    region: crosscache::Region::Arena as i32,
                    offset: 0,
                    bytes: 4096,
                }],
                transfer: 0,
            })),
        })
        .await
        .unwrap();
    let refused = replies.message().await.unwrap().unwrap();
    assert!(matches!(
        refused.r,
        Some(crosscache::get_reply::R::Refused(crosscache::Refused {
            reason,
            ..
        })) if reason == crosscache::Reason::Invalid as i32
    ));
}

#[test]
fn l0_14_throughput() {
    support::throughput::run(false);
}

#[test]
fn l0_15_namespace_id_vectors() {
    let id = record::namespace_id(&[0x63; 32], &[0x1b; 16], "test");
    assert_eq!(
        record::hex(&id),
        "010a5fe4fa734a28b9b50699d208f60110b851e60a87ebc6e576d675f29aebb4"
    );
    let id = record::namespace_id(&[0x63; 32], &[0x1b; 16], "");
    assert_eq!(
        record::hex(&id),
        "ced386eec7bca02f955a4e337b4d950c0a12dc9f7b61ab9b08a30a82971f330c"
    );
}

#[test]
fn l0_16_disk_queue_read_write_interleave() {
    let base = base("disk");
    fs::create_dir_all(&base).unwrap();
    let lock = Arc::new(disk::RootLock::acquire(&base.join("root")).unwrap());
    let faults = disk::Faults {
        delay_ms: 2,
        ..disk::Faults::default()
    };
    let worker = disk::Worker::start(faults, lock);
    let source = base.join("source");
    let mut bytes = vec![0u8; 4096];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = index as u8;
    }
    fs::write(&source, &bytes).unwrap();
    let file = Arc::new(disk::open_read(&source).unwrap());
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let arena = Arena::new(8 * 4096).unwrap();
    let reader = worker.reader();
    let write_arena = Arena::new(4 * 4096).unwrap();
    for index in 0..4 {
        let mut extent = write_arena.allocate(4096).unwrap();
        extent.bytes_mut().fill(index as u8);
        let temporary = Arc::new(disk::DiskExtent::new(base.join(format!("w-{index}.tmp"))));
        temporary.retire();
        worker
            .sender
            .send(disk::Task::Chunk {
                id: index,
                temporary,
                offset: 0,
                data: Arc::new(extent),
            })
            .unwrap();
    }
    let mut readers = Vec::new();
    for index in 0..8 {
        let reader = reader.clone();
        let file = file.clone();
        let log = log.clone();
        let extent = arena.allocate(4096).unwrap();
        readers.push(std::thread::spawn(move || {
            reader.read(&file, 0, extent).unwrap();
            log.lock().unwrap().push((index, b'R'));
        }));
    }
    for _ in 0..4 {
        let completion = worker.receiver.recv().unwrap();
        log.lock().unwrap().push((completion.id, b'W'));
    }
    for reader in readers {
        reader.join().unwrap();
    }
    let log = log.lock().unwrap();
    let mut run = 0;
    let mut saw_write = false;
    for &(_, kind) in log.iter() {
        if kind == b'R' {
            run += 1;
            assert!(
                !saw_write || run <= 4,
                "more than four reads between writes"
            );
        } else {
            saw_write = true;
            run = 0;
        }
    }
    fs::remove_dir_all(base).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_17_write_budget_and_room() {
    let base = base("budget");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        64 * 1024 * 1024,
        64 * 1024 * 1024,
    );
    config.name = instance_name("budget");
    config.write_budget = 1024 * 1024;
    let mut host = Host::open_config(config);
    host.start();
    let mut a = host.client().await;
    let mut subscription = a.subscribe().await;
    a.store(key(128), None).await;
    a.store(key(2048 * 64), None).await;
    // Gets are never throttled by the write budget.
    a.fetch(&[key(128)], 0, 2).await;
    assert_eq!(
        a.put(key(256), None, PutOptions::default()).await,
        PutOutcome::Refused(crosscache::Reason::Budget as u8)
    );
    let room = subscription.until_room().await;
    assert!(room > 0);
    let _ = a.store(key(256), None).await;
    // Buckets are independent per connection.
    let mut b = host.client().await;
    b.store(key(384), None).await;
    // 0 means unlimited.
    let mut unlimited = Host::new(64 * 1024 * 1024, 64 * 1024 * 1024);
    unlimited.start();
    let mut c = unlimited.client().await;
    c.store(key(2048 * 64), None).await;
    c.store(key(2048 * 64 + 64), Some(key(2048 * 64))).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_18_descriptor_format_and_recovery_refusal() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    let directory = host
        .config
        .root
        .as_ref()
        .unwrap()
        .join(client.descriptor.name());
    let bytes = fs::read(directory.join("descriptor")).unwrap();
    assert_eq!(&bytes[..8], b"FLCDSC2\0");
    let length = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    assert_eq!(bytes.len(), 16 + length);
    assert_eq!(
        disk::crc32(&bytes[..12 + length]),
        u32::from_le_bytes(bytes[12 + length..].try_into().unwrap())
    );
    host.stop();
    let root = host.config.root.clone().unwrap();
    let v3 = root.join("v3-root");
    fs::create_dir_all(&v3).unwrap();
    let error = disk::recover(&root).err().expect("recover must fail");
    assert!(error.to_string().contains(&v3.display().to_string()));
    assert!(error.to_string().contains("not migratable"));
    fs::remove_dir_all(&v3).unwrap();
    let other = record::namespace_id(&[1; 32], &[2; 16], "");
    let other = root.join(record::hex(&other));
    fs::create_dir_all(&other).unwrap();
    fs::write(other.join("descriptor"), b"FLCDSC1\0....").unwrap();
    let error = disk::recover(&root).err().expect("recover must fail");
    assert!(
        error
            .to_string()
            .contains(&other.join("descriptor").display().to_string())
    );
    assert!(error.to_string().contains("not migratable"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_19_two_managers_one_name() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let base = base("second");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        1024 * 1024,
        1024 * 1024,
    );
    config.name = host.config.name.clone();
    let mut second = Host::open_config(config);
    assert!(second.try_start().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_20_size_mismatch_before_mmap() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let client = host.client().await;
    let path = client.staging_path.clone();
    drop(client);
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(4096)
        .unwrap();
    let error = Client::connect(&host.config, descriptor())
        .await
        .err()
        .unwrap();
    assert!(error.contains("size mismatch"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_21_live_disconnected_client_quarantine() {
    if let Ok(socket) = std::env::var(CHILD_QUARANTINE) {
        let mut config = Config::dram_only(PathBuf::from(&socket), 32768);
        config.name = instance_name("quarantine-child");
        let mut client = Client::connect(&config, descriptor()).await.unwrap();
        let _ = client
            .get(
                &[key(128)],
                0,
                2,
                GetOptions {
                    cancel: Some(CancelAt::Grant),
                    ..GetOptions::default()
                },
            )
            .await;
        drop(client);
        fs::write(PathBuf::from(&socket).with_extension("ready"), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }
    let mut host = Host::dram_only(32768);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    assert!(host.gauge("memory_allocated_bytes") > 0);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "l0_21_live_disconnected_client_quarantine",
            "--nocapture",
        ])
        .env(CHILD_QUARANTINE, &host.config.socket)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let ready = host.config.socket.with_extension("ready");
    wait(|| ready.exists());
    // The connection is gone but the process lives: the lease is quarantined.
    host.wait(|host| host.gauge("connections") <= 1);
    assert!(host.gauge("memory_allocated_bytes") > 0);
    let mut probe = host.client().await;
    assert_eq!(
        probe.put(key(256), None, PutOptions::default()).await,
        PutOutcome::Refused(crosscache::Reason::Budget as u8)
    );
    child.kill().unwrap();
    child.wait().unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match probe.put(key(256), None, PutOptions::default()).await {
            PutOutcome::Stored(_) => break,
            PutOutcome::Refused(reason) if reason == crosscache::Reason::Budget as u8 => {
                assert!(Instant::now() < deadline, "quarantine was not reclaimed");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            other => panic!("unexpected Put outcome after exit: {other:?}"),
        }
    }
    fs::remove_file(ready).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_22_cancel_every_state_and_commit_wins() {
    let mut host = Host::new(4096, 4 * 1024 * 1024);
    host.start();
    for cancel in [
        CancelAt::Accept,
        CancelAt::Grant,
        CancelAt::Filled,
        CancelAt::Commit,
    ] {
        let mut client = host.client().await;
        let value = key(match cancel {
            CancelAt::Accept => 192,
            CancelAt::Grant => 256,
            CancelAt::Filled => 320,
            CancelAt::Commit => 384,
            CancelAt::None => 128,
        });
        let outcome = client
            .put(
                value,
                None,
                PutOptions {
                    cancel: Some(cancel),
                    ..PutOptions::default()
                },
            )
            .await;
        match cancel {
            CancelAt::Commit => assert!(matches!(outcome, PutOutcome::Stored(_))),
            _ => assert!(
                matches!(outcome, PutOutcome::Cancelled),
                "{cancel:?}: {outcome:?}"
            ),
        }
    }
    let mut client = host.client().await;
    let bytes = client.store(key(128), None).await;
    assert_eq!(
        client
            .get(
                &[key(128)],
                0,
                2,
                GetOptions {
                    cancel: Some(CancelAt::Grant),
                    ..GetOptions::default()
                },
            )
            .await,
        GetOutcome::Cancelled
    );
    let mut after = host.client().await;
    assert_eq!(after.fetch(&[key(128)], 0, 2).await[0], bytes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_23_commit_waits_for_durability() {
    let base = base("commit");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("commit");
    config.faults.pause_at = Some("before_rename".into());
    let signal = base.join("gate");
    config.faults.signal = Some(signal.clone());
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let stored = Arc::new(AtomicBool::new(false));
    let flag = stored.clone();
    let task = tokio::spawn(async move {
        let outcome = client.put(key(128), None, PutOptions::default()).await;
        flag.store(true, Ordering::Release);
        outcome
    });
    wait(|| signal.exists());
    assert!(!stored.load(Ordering::Acquire), "Stored before durability");
    assert_eq!(host.durable_records(), 0);
    fs::remove_file(signal).unwrap();
    assert!(matches!(task.await.unwrap(), PutOutcome::Stored(_)));
    host.durable(1);
}

#[test]
fn l0_24_signal_handling() {
    let base = base("signal");
    fs::create_dir_all(base.join("run")).unwrap();
    let socket = base.join("run/manager.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_flare-crosscache"))
        .args([
            "--name",
            &instance_name("signal"),
            "--socket",
            socket.to_str().unwrap(),
            "--memory",
            "8M",
            "--staging",
            "8M",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    wait(|| socket.exists());
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    let status = child.wait().unwrap();
    assert!(status.success(), "{status:?}");
    assert!(!socket.exists());
    let _ = fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_25_verify_before_window_reuse() {
    let mut host = Host::new(4096, 4 * 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let outcome = client
        .put(
            key(128),
            None,
            PutOptions {
                corrupt_page: Some(0),
                ..PutOptions::default()
            },
        )
        .await;
    assert_eq!(
        outcome,
        PutOutcome::Refused(crosscache::Reason::Invalid as u8)
    );
    host.wait(|host| host.gauge("staging_allocated_bytes") == 0);
    let mut fresh = host.client().await;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match fresh.put(key(128), None, PutOptions::default()).await {
            PutOutcome::Stored(_) => break,
            PutOutcome::Refused(reason) if reason == crosscache::Reason::Inflight as u8 => {
                assert!(Instant::now() < deadline, "inflight did not drain");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_26_namespace_registration_and_mismatch() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    // A descriptor with the same namespace id but different geometry is a
    // precondition failure, not a panic.
    let mut mismatch = descriptor();
    mismatch.ranks[0].row_bytes += 1;
    assert_eq!(mismatch.namespace_id(), client.descriptor.namespace_id());
    let status = client.subscribe_descriptor(&mismatch).await.err().unwrap();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    // An unregistered namespace on a Put is also a precondition failure.
    let mut other = descriptor();
    other.fingerprint = [11; 16];
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(crosscache::PutMessage {
            m: Some(crosscache::put_message::M::Begin(crosscache::PutBegin {
                namespace_id: other.namespace_id().to_vec(),
                key: Some(key_msg(key(64))),
                chain_pages: 1,
                row: true,
                published_parent: None,
                has_published_parent: false,
                bucket: vec![4; 16],
                published_tail_pages: 1,
                descriptor: Some(descriptor_msg(&other)),
            })),
        })
        .await
        .unwrap();
    let status = replies.message().await.err().unwrap();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    // The manager is still healthy.
    assert!(!client.fetch(&[key(128)], 0, 2).await[0].is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_27_no_progress_timeout_ends_the_call() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(crosscache::PutMessage {
            m: Some(client.begin(key(256), None)),
        })
        .await
        .unwrap();
    loop {
        match replies.message().await.unwrap().unwrap().r {
            Some(crosscache::put_reply::R::Accept(_)) => break,
            Some(_) => continue,
            None => panic!("reply stream ended"),
        }
    }
    let start = Instant::now();
    let mut timed_out = false;
    loop {
        match replies.message().await {
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
                timed_out = true;
                break;
            }
            Ok(Some(reply)) => match reply.r {
                Some(crosscache::put_reply::R::Refused(refused)) => {
                    assert_eq!(refused.reason, crosscache::Reason::Invalid as i32);
                    timed_out = true;
                    break;
                }
                Some(_) => continue,
                None => break,
            },
            Ok(None) => break,
        }
    }
    assert!(timed_out, "the call did not end on the no-progress timer");
    assert!(start.elapsed() >= Duration::from_secs(2));
    assert!(start.elapsed() < Duration::from_secs(8));
    drop(sender);
    let mut fresh = host.client().await;
    assert!(!fresh.fetch(&[key(128)], 0, 2).await[0].is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_28_rootless_eviction_skips_internal_nodes() {
    let mut host = Host::dram_only(65536);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    client.store(key(192), Some(key(128))).await;
    // The arena is full and the root now has a child. Eviction must take the
    // leaf child, not loop on the row-bearing internal root.
    let stored = tokio::time::timeout(Duration::from_secs(5), client.store(key(256), None))
        .await
        .expect("rootless eviction must make progress");
    assert!(!stored.is_empty());
    let subscription = client.subscribe().await;
    assert!(subscription.entries.contains_key(&key(256)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_29_cancel_retires_blocked_chunks() {
    let base = base("blocked");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("blocked");
    config.faults.delay_ms = 300;
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let outcome = client
        .put(
            key(1024 * 64),
            None,
            PutOptions {
                cancel_after_fills: Some(12),
                ..PutOptions::default()
            },
        )
        .await;
    assert_eq!(outcome, PutOutcome::Cancelled);
    // The cancelled Put must release INFLIGHT and its reservation; blocked
    // chunks that were never submitted must not hold the drain open.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut probe = host.client().await;
    loop {
        let outcome = probe
            .put(
                key(1024 * 64),
                None,
                PutOptions {
                    cancel: Some(CancelAt::Accept),
                    ..PutOptions::default()
                },
            )
            .await;
        match outcome {
            PutOutcome::Refused(reason) if reason == crosscache::Reason::Inflight as u8 => {
                assert!(Instant::now() < deadline, "INFLIGHT did not drain");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            PutOutcome::Cancelled => break,
            other => panic!("unexpected probe outcome: {other:?}"),
        }
    }
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_30_committed_detached_put_finishes() {
    let base = base("detached");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("detached");
    config.faults.delay_ms = 100;
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let outcome = client
        .put(
            key(512 * 64),
            None,
            PutOptions {
                abort_after_commit: true,
                ..PutOptions::default()
            },
        )
        .await;
    assert_eq!(outcome, PutOutcome::Abandoned);
    drop(client);
    host.wait(|host| host.durable_records() == 1);
    let mut check = host.client().await;
    let subscription = check.subscribe().await;
    assert!(subscription.entries.contains_key(&key(512 * 64)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_31_timed_out_publication_finishes_instead_of_orphaning() {
    let base = base("timeout-publish");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("timeout-publish");
    config.faults.pause_at = Some("before_rename".into());
    let signal = base.join("gate");
    config.faults.signal = Some(signal.clone());
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let task = tokio::spawn(async move { client.put(key(128), None, PutOptions::default()).await });
    let deadline = Instant::now() + Duration::from_secs(10);
    while !signal.exists() {
        assert!(
            Instant::now() < deadline,
            "publication never reached before_rename"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Let the 3 s no-progress timer end the call while the rename is paused.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(host.durable_records(), 0);
    fs::remove_file(&signal).unwrap();
    let outcome = task.await.unwrap();
    assert!(
        matches!(
            outcome,
            PutOutcome::Status(_) | PutOutcome::Cancelled | PutOutcome::Abandoned
        ),
        "{outcome:?}"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while host.durable_records() != 1 {
        assert!(Instant::now() < deadline, "publication did not complete");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut check = host.client().await;
    let subscription = check.subscribe().await;
    assert!(subscription.entries.contains_key(&key(128)));
    // Recovery must find exactly the record the catalog announced.
    drop(subscription);
    drop(check);
    host.stop();
    eprintln!("l0_31: stopped");
    host.start();
    let mut after = host.client().await;
    let subscription = after.subscribe().await;
    assert_eq!(subscription.entries.len(), 1);
    assert!(subscription.entries.contains_key(&key(128)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_32_streaming_reservation_includes_the_disk_record() {
    let mut host = Host::new(4096, 64 * 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let (sender, replies) = client.raw_begin(key(128), None).await;
    // key(128) is 2 pages + row: page_segment 8192, row_segment 16384,
    // row 4096, so envelope + 20480 = 24576 is reserved before any staging.
    let deadline = Instant::now() + Duration::from_secs(5);
    while host.gauge("nvme_reserved_bytes") < 24576 {
        assert!(
            Instant::now() < deadline,
            "streaming reservation was undercharged"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    sender
        .send(crosscache::PutMessage {
            m: Some(crosscache::put_message::M::Cancel(crosscache::Cancel {})),
        })
        .await
        .unwrap();
    drop(sender);
    drop(replies);
    drop(client);
    let deadline = Instant::now() + Duration::from_secs(10);
    while host.gauge("nvme_reserved_bytes") >= 24576 {
        assert!(
            Instant::now() < deadline,
            "streaming reservation was not released"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_33_write_budget_uses_the_streaming_charge() {
    let base = base("stream-budget");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("stream-budget");
    config.write_budget = 1024 * 1024;
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let mut subscription = client.subscribe().await;
    // 8 MiB streamed record: admitted with a negative balance.
    client.store(key(2048 * 64), None).await;
    assert_eq!(
        client.put(key(256), None, PutOptions::default()).await,
        PutOutcome::Refused(crosscache::Reason::Budget as u8)
    );
    let room = subscription.until_room().await;
    assert!(room > 0);
    client.store(key(256), None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_34_concurrent_nvme_gets_share_connection_credits() {
    let mut host = Host::new(4096, 64 * 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let one = client.store(key(64 * 64), None).await;
    let two = client.store(key(128 * 64), None).await;
    let (left, right) = client
        .fetch_two((vec![key(64 * 64)], 0, 64), (vec![key(128 * 64)], 0, 128))
        .await;
    match (left, right) {
        (GetOutcome::Done(records), GetOutcome::Done(other)) => {
            assert_eq!(records[0], one);
            assert_eq!(other[0], two);
        }
        other => panic!("concurrent Gets failed: {other:?}"),
    }
    host.wait(|host| {
        host.gauge("leases") == 0
            && host.gauge("cleanup_records") == 0
            && host.gauge("open_ops") == 0
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_35_generation_fencing_reaches_the_owner_thread() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    // Open a Put stream under generation 1 but do not send its begin yet.
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    // A second Hello on the same transport supersedes generation 1.
    let hello = client
        .inner
        .hello(tonic::Request::new(crosscache::HelloRequest {
            protocol: 1,
            replica: "again".into(),
            descriptor: Some(descriptor_msg(&client.descriptor)),
        }))
        .await
        .unwrap()
        .into_inner();
    client.generation = hello.generation;
    // The old stream must not be able to begin into the new session.
    sender
        .send(crosscache::PutMessage {
            m: Some(client.begin(key(256), None)),
        })
        .await
        .unwrap();
    let status = loop {
        match replies.message().await {
            Err(status) => break status,
            Ok(Some(reply)) => match reply.r {
                Some(crosscache::put_reply::R::Refused(_)) => {
                    panic!("stale stream reached the new session")
                }
                _ => continue,
            },
            Ok(None) => panic!("stale stream ended without a status"),
        }
    };
    assert_eq!(status.code(), tonic::Code::Aborted);
    drop(sender);
    drop(replies);
    let mut fresh = host.client().await;
    fresh.store(key(256), None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_36_connection_limit_is_enforced() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut clients = Vec::new();
    for _ in 0..64 {
        clients.push(host.client().await);
    }
    assert!(
        host.client_with(descriptor()).await.is_err(),
        "the 65th connection must be refused"
    );
    // Dropping one transport frees its slot.
    clients.pop();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match host.client_with(descriptor()).await {
            Ok(client) => {
                clients.push(client);
                break;
            }
            Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "connection slot was not released"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_37_subscription_epoch_discards_queued_events() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let mut first = client.subscribe().await;
    let mut second = client.subscribe().await;
    // After the replacement the old epoch must not deliver new events.
    client.store(key(128), None).await;
    second.until_added(key(128)).await;
    match first.stream.message().await {
        Ok(None) => {}
        Ok(Some(event)) => panic!("stale subscription yielded {event:?}"),
        Err(_) => {}
    }
}

#[test]
fn l0_38_async_reads_overtake_queued_writes() {
    let base = base("read-priority");
    fs::create_dir_all(&base).unwrap();
    let lock = Arc::new(disk::RootLock::acquire(&base.join("root")).unwrap());
    let worker = disk::Worker::start(
        disk::Faults {
            delay_ms: 100,
            ..disk::Faults::default()
        },
        lock,
    );
    let source = base.join("source");
    fs::write(&source, vec![7u8; 4096]).unwrap();
    let file = Arc::new(disk::open_read(&source).unwrap());
    let arena = Arena::new(4096).unwrap();
    let write_arena = Arena::new(8 * 4096).unwrap();
    for index in 0..8u64 {
        let mut extent = write_arena.allocate(4096).unwrap();
        extent.bytes_mut().fill(index as u8);
        let temporary = Arc::new(disk::DiskExtent::new(base.join(format!("w-{index}.tmp"))));
        temporary.retire();
        worker
            .sender
            .send(disk::Task::Chunk {
                id: index,
                temporary,
                offset: 0,
                data: Arc::new(extent),
            })
            .unwrap();
    }
    worker
        .read_sender
        .send(disk::Task::Read {
            id: 999,
            file,
            offset: 0,
            buffer: arena.allocate(4096).unwrap(),
        })
        .unwrap();
    let first = worker.receiver.recv().unwrap();
    assert_eq!(
        first.id, 999,
        "the read must not wait behind the write queue"
    );
    assert_eq!(first.data.unwrap().bytes(), &[7u8; 4096]);
    for _ in 0..8 {
        worker.receiver.recv().unwrap();
    }
    fs::remove_dir_all(base).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_39_gap_recovery_uses_a_fresh_snapshot() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let mut first = client.subscribe().await;
    client.store(key(128), None).await;
    client.store(key(256), Some(key(128))).await;
    first.until_added(key(256)).await;
    // The client detects a missing event / gap and rebuilds with a fresh
    // Subscribe (gRPC has no RESYNC); the new snapshot must contain both.
    drop(first);
    let mut second = client.subscribe().await;
    assert!(second.entries.contains_key(&key(128)));
    assert!(second.entries.contains_key(&key(256)));
    client.store(key(384), Some(key(256))).await;
    second.until_added(key(384)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_40_detached_finalization_uses_the_put_namespace() {
    let base = base("detached-ns");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("detached-ns");
    config.faults.delay_ms = 50;
    let mut host = Host::open_config(config);
    host.start();
    let mut first = host.client().await;
    let mut other_descriptor = descriptor();
    other_descriptor.fingerprint = [9; 16];
    let mut other = host.client_with(other_descriptor.clone()).await.unwrap();
    let _other_subscription = other.subscribe().await;
    // Parent in the first namespace, then a child whose connection goes away
    // after commit. Finalization must use the Put's namespace, not an
    // arbitrary one.
    first.store(key(64), None).await;
    host.wait(|host| host.durable_records() == 1);
    let outcome = first
        .put(
            key(576 * 64),
            Some(key(64)),
            PutOptions {
                abort_after_commit: true,
                ..PutOptions::default()
            },
        )
        .await;
    assert_eq!(outcome, PutOutcome::Abandoned);
    drop(first);
    host.wait(|host| host.durable_records() == 2);
    drop(_other_subscription);
    drop(other);
    host.stop();
    host.start();
    let mut first = host.client().await;
    let subscription = first.subscribe().await;
    assert!(subscription.entries.contains_key(&key(64)));
    assert!(subscription.entries.contains_key(&key(576 * 64)));
    let mut other = host.client_with(other_descriptor.clone()).await.unwrap();
    let subscription = other.subscribe().await;
    assert!(subscription.entries.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_41_streaming_windows_respect_the_nvme_cap() {
    let base = base("staging-cap");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        24 * 1024,
    );
    config.name = instance_name("staging-cap");
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    // The disk record fits the cap but its first staging window does not.
    assert_eq!(
        client.put(key(128), None, PutOptions::default()).await,
        PutOutcome::Refused(crosscache::Reason::Budget as u8)
    );
    assert!(host.gauge("nvme_reserved_bytes") <= 24 * 1024);
    // A row-only record whose window does fit still streams.
    let _ = client.store(key(3), None).await;
    assert!(host.gauge("nvme_reserved_bytes") <= 24 * 1024);
}

mod support;
use flare_crosscache::{
    arena::Arena,
    codec::Decoder,
    disk,
    protocol::{Frame, tag},
    record,
    store::{self, Config, DispatchOutcome, PutFrameInput, PutFrameOutcome},
    tree::{Eviction, Tree},
};
use std::{
    fs,
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use support::*;

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
    let decoded = record::Header::decode(fixture).unwrap();
    let mut planned =
        record::Header::new(&descriptor(), key(130), Some(key(65)), [4; 16], 1).unwrap();
    planned.created = 1_700_000_000;
    planned.row_checksum = 0x0123456789abcdef;
    assert_eq!(decoded, planned);
    assert_eq!(&encode_header(&planned), fixture);
    for (offset, value) in [(129, 0u64), (173, 0), (181, u64::MAX)] {
        let mut invalid = *fixture;
        invalid[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        assert!(record::Header::decode(&invalid).is_err());
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
    let mut host = StoreHost::new(65536, 49152);
    let mut c = host.connect();
    host.store_record(&mut c, key(128), None);
    host.store_record(&mut c, key(256), Some(key(128)));
    host.durable(2);
    let (mut op, mut answer) = host.put(&mut c, key(512), None);
    while answer.tag == tag::RETRY {
        host.until_room();
        (op, answer) = host.put(&mut c, key(512), None);
    }
    assert_eq!(answer.tag, tag::ACCEPT);
    host.cancel(&c, op);
    assert!(!host.entries().contains_key(&key(256)));
}

#[test]
fn l0_03_accept_shapes_and_compacted_replacement() {
    let mut host = StoreHost::new(1024 * 1024, 98304);
    let mut c = host.connect();
    let (op, answer) = host.put(&mut c, key(448), Some(key(384)));
    assert_eq!(answer.tag, tag::ACCEPT);
    assert_eq!(answer.payload, [0, 0, 0, 0, 0]);
    host.cancel(&c, op);
    host.store_record(&mut c, key(128), None);
    host.store_record(&mut c, key(256), Some(key(128)));
    host.durable(2);
    // Pin the leaf with a live accepted child, forcing its single-child ancestor to compact.
    let (child, answer) = host.put(&mut c, key(320), Some(key(256)));
    assert_eq!(answer.tag, tag::ACCEPT);
    let (_, answer) = host.put(&mut c, key(640), None);
    assert_eq!(answer.tag, tag::RETRY);
    host.wait(|host| host.gauge("compactions_total") > 0);
    std::thread::sleep(Duration::from_millis(20));
    assert!(!host.entries()[&key(128)].row);
    let (op, answer) = loop {
        host.pump();
        let answer = host.put(&mut c, key(128), None);
        if answer.1.tag != tag::RETRY {
            break answer;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(answer.tag, tag::ACCEPT);
    let mut accepted = Decoder::new(&answer.payload);
    let first = accepted.u32().unwrap();
    let shaped_parent = accepted.parent().unwrap();
    let (header, bytes, hashes) = content_for(&c.descriptor, key(128), shaped_parent, first);
    host.upload(&c, op, &header, &bytes, &hashes).unwrap();
    host.wait_put(op).unwrap();
    assert!(host.entries()[&key(128)].row);
    assert_eq!(host.entries()[&key(256)].parent, Some(key(128)));
    host.cancel(&c, child);
}

#[test]
fn l0_04_present_and_inflight_dedup() {
    let mut host = StoreHost::new(1024 * 1024, 1024 * 1024);
    let mut a = host.connect();
    let mut b = host.connect();
    let (op, answer) = host.put(&mut a, key(128), None);
    assert_eq!(answer.tag, tag::ACCEPT);
    let (_, answer) = host.put(&mut b, key(128), None);
    assert_eq!(answer.tag, tag::RETRY);
    assert_eq!(answer.payload, [2]);
    let (header, bytes, hashes) = content_for(&a.descriptor, key(128), None, 0);
    host.upload(&a, op, &header, &bytes, &hashes).unwrap();
    host.wait_put(op).unwrap();
    assert_eq!(host.put(&mut b, key(128), None).1.tag, tag::PRESENT);
    // Four open puts per direction, independent of key dedup.
    for tokens in [256, 384, 512, 640] {
        assert_eq!(host.put(&mut a, key(tokens), None).1.tag, tag::ACCEPT);
    }
    let answer = host.put(&mut a, key(768), None).1;
    assert_eq!(answer.tag, tag::RETRY);
    assert_eq!(answer.payload, [0]);
    host.close(&a);
    host.wait(|host| host.gauge("open_ops") == 0);
    let mut c = host.connect();
    host.store_record(&mut c, key(3), None);
    host.store_record(&mut c, key(5), Some(key(3)));
    assert_eq!(host.entries()[&key(5)].tail_pages, 0);
    host.fetch(&mut c, &[key(3), key(5)], 0, 0);
}

#[test]
fn l0_05_invalid_checksum_short_payload_and_offsets() {
    let mut host = StoreHost::new(1024 * 1024, 1024 * 1024);
    let mut c = host.connect();
    for (index, tokens) in [128, 256, 384, 512, 640, 768].into_iter().enumerate() {
        let k = key(tokens);
        let (op, answer) = host.put(&mut c, k, None);
        assert_eq!(answer.tag, tag::ACCEPT);
        let (header, mut bytes, hashes) = content_for(&c.descriptor, k, None, 0);
        if index == 0 {
            bytes[header.page_segment as usize] ^= 1;
            assert!(host.upload(&c, op, &header, &bytes, &hashes).is_err());
        } else if index == 1 {
            assert!(matches!(
                host.store_mut().put_frame(
                    c.id,
                    op,
                    PutFrameInput::Commit {
                        offset: 0,
                        metadata: &bytes[..header.page_segment as usize],
                    },
                ),
                PutFrameOutcome::Error(_)
            ));
        } else if index == 2 {
            assert!(matches!(
                host.store_mut().put_frame(
                    c.id,
                    op,
                    PutFrameInput::Window {
                        index: 0,
                        pages: vec![hashes[0]],
                    },
                ),
                PutFrameOutcome::Ok
            ));
            assert!(matches!(
                host.store_mut().put_frame(
                    c.id,
                    op,
                    PutFrameInput::Data {
                        offset: header.page_segment + 4096,
                        data: &bytes
                            [header.page_segment as usize..header.page_segment as usize + 4096],
                    },
                ),
                PutFrameOutcome::Error(_)
            ));
        } else {
            if index == 3 {
                bytes[header.row_segment as usize] ^= 1;
            }
            if index == 4 {
                bytes[12] = 1;
            }
            if index == 5 {
                bytes[4096] ^= 1;
            }
            assert!(host.upload(&c, op, &header, &bytes, &hashes).is_err());
        }
        host.cancel(&c, op);
        assert!(!host.entries().contains_key(&k));
    }
    host.wait(|host| {
        host.gauge("nvme_reserved_bytes") == 0 && host.gauge("memory_allocated_bytes") == 0
    });
}

#[test]
fn l0_06_parent_first_chain_closed_durability() {
    let base = base("store");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4 * 1024 * 1024,
        4 * 1024 * 1024,
    );
    config.faults.pause_at = Some("parent_write".into());
    let signal = base.join("gate");
    config.faults.signal = Some(signal.clone());
    let mut host = StoreHost::open(config);
    let mut c = host.connect();
    host.store_record(&mut c, key(128), None);
    host.wait(|_| signal.exists());
    host.store_record(&mut c, key(256), Some(key(128)));
    assert!(
        !host
            .config
            .root
            .join(c.descriptor.name())
            .join(record::file_name(&key(256)))
            .exists()
    );
    assert_eq!(host.gauge("nvme_bytes_total"), 0);
    fs::remove_file(&signal).unwrap();
    // Every write pauses independently; parent must be installed before child's pause.
    host.wait(|_| signal.exists());
    assert!(
        host.config
            .root
            .join(c.descriptor.name())
            .join(record::file_name(&key(128)))
            .exists()
    );
    assert!(
        !host
            .config
            .root
            .join(c.descriptor.name())
            .join(record::file_name(&key(256)))
            .exists()
    );
    fs::remove_file(signal).unwrap();
    host.durable(2);
    assert!(host.entries().values().all(|entry| entry.durable));
}

#[test]
fn l0_07_multiprocess_kill_recovery() {
    if let Ok(base) = std::env::var("CM_CHILD_BASE") {
        let base = std::path::PathBuf::from(base);
        let mut config = Config::new(
            base.join("records"),
            base.join("run/m.sock"),
            4 * 1024 * 1024,
            64 * 1024 * 1024,
        );
        config.faults.pause_at = std::env::var("CM_CHILD_PAUSE").ok();
        config.faults.signal = Some(base.join("gate"));
        let mut host = StoreHost::open(config);
        if let Ok(dump) = std::env::var("CM_CHILD_DUMP") {
            fs::write(dump, format!("{}", host.snapshot_keys().len())).unwrap();
            loop {
                std::thread::park();
            }
        }
        let mut c = host.connect();
        let point = std::env::var("CM_CHILD_PAUSE").unwrap_or_default();
        if point == "data" {
            let (op, answer) = host.put(&mut c, key(128), None);
            assert_eq!(answer.tag, tag::ACCEPT);
            let mut accepted = Decoder::new(&answer.payload);
            let first = accepted.u32().unwrap();
            let parent = accepted.parent().unwrap();
            let (header, bytes, hashes) = content_for(&c.descriptor, key(128), parent, first);
            host.store_mut().put_frame(
                c.id,
                op,
                PutFrameInput::Window {
                    index: 0,
                    pages: hashes,
                },
            );
            host.store_mut().put_frame(
                c.id,
                op,
                PutFrameInput::Data {
                    offset: header.page_segment,
                    data: &bytes[header.page_segment as usize..header.page_segment as usize + 4096],
                },
            );
        } else {
            host.store_record(&mut c, key(128), None);
            if point == "parent_write" {
                host.store_record(&mut c, key(256), Some(key(128)));
            }
        }
        loop {
            host.pump();
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    for point in ["data", "before_rename", "after_rename", "parent_write"] {
        let base = base(point);
        let config = Config::new(
            base.join("records"),
            base.join("run/m.sock"),
            4 * 1024 * 1024,
            64 * 1024 * 1024,
        );
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "l0_07_multiprocess_kill_recovery", "--nocapture"])
            .env("CM_CHILD_BASE", &base)
            .env("CM_CHILD_PAUSE", point)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        wait(|| base.join("gate").exists());
        child.kill().unwrap();
        let status = child.wait().unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        let lock = disk::RootLock::acquire(&config.root).unwrap();
        let recovered = disk::recover(&config.root).unwrap();
        let keys: Vec<_> = recovered
            .iter()
            .flat_map(|namespace| namespace.records.iter().map(|(header, _, _)| header.key))
            .collect();
        assert_eq!(
            keys,
            if point == "after_rename" {
                vec![key(128)]
            } else {
                vec![]
            },
            "{point}"
        );
        assert!(
            fs::read_dir(config.root.join(descriptor().name()))
                .unwrap()
                .all(|entry| !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp"))
        );
        drop(lock);
        // Start a real second process over the same disk root and verify its snapshot.
        let _ = fs::remove_file(base.join("gate"));
        let dump = base.join("dump");
        let mut restarted = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "l0_07_multiprocess_kill_recovery", "--nocapture"])
            .env("CM_CHILD_BASE", &base)
            .env("CM_CHILD_DUMP", &dump)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        wait(|| dump.exists());
        assert_eq!(
            fs::read_to_string(&dump).unwrap().parse::<usize>().unwrap(),
            keys.len()
        );
        restarted.kill().unwrap();
        restarted.wait().unwrap();
        fs::remove_dir_all(base).unwrap();
    }
}

#[test]
fn l0_08_streaming_and_nvme_oversize() {
    let mut host = StoreHost::new(65536, 2 * 1024 * 1024);
    let mut c = host.connect();
    let bytes = host.store_record(&mut c, key(128 * 64), None);
    host.durable(1);
    assert_eq!(host.gauge("memory_allocated_bytes"), 0);
    let data = host.fetch(&mut c, &[key(128 * 64)], 0, 128);
    let mut actual = vec![0; bytes.len()];
    for (_, offset, chunk) in data {
        actual[offset as usize..offset as usize + chunk.len()].copy_from_slice(&chunk);
    }
    assert_eq!(actual, bytes);
    assert_eq!(host.put(&mut c, key(1024 * 64), None).1.tag, tag::OVERSIZE);
    host.restart();
    let mut c = host.connect();
    assert_eq!(host.entries().len(), 1);
    host.fetch(&mut c, &[key(128 * 64)], 64, 64);
    let mut refill = StoreHost::new(65536, 1024 * 1024);
    let mut c = refill.connect();
    refill.store_record(&mut c, key(128), None);
    refill.durable(1);
    refill.restart();
    let mut c = refill.connect();
    assert_eq!(refill.entries()[&key(128)].tier, 1);
    refill.fetch(&mut c, &[key(128)], 0, 2);
    refill.wait(|host| host.gauge("memory_allocated_bytes") > 0);
    assert_eq!(refill.entries()[&key(128)].tier, 0);
    let mut split = StoreHost::new(4096, 32 * 1024 * 1024);
    let mut geometry = descriptor();
    geometry.ranks[0].planes = vec![flare_crosscache::protocol::MAX_DATA as u64 + 4096];
    let mut c = split.connect_with(geometry);
    let expected = split.store_record(&mut c, key(128), None);
    let mut actual = vec![0; expected.len()];
    for (_, offset, data) in split.fetch(&mut c, &[key(128)], 0, 2) {
        actual[offset as usize..offset as usize + data.len()].copy_from_slice(&data);
    }
    assert_eq!(actual, expected);
}

#[test]
fn l0_09_disk_full_budget_and_room() {
    let base = base("store");
    let full = base.join("full");
    fs::create_dir_all(&base).unwrap();
    fs::write(&full, b"full").unwrap();
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        65536,
        1024 * 1024,
    );
    config.faults.disk_full = Some(full.clone());
    let mut host = StoreHost::open(config);
    let mut c = host.connect();
    host.store_record(&mut c, key(384), None);
    host.wait(|host| host.gauge("nvme_errors_total") > 0);
    let (_, answer) = host.put(&mut c, key(448), None);
    assert_eq!(answer.tag, tag::RETRY);
    assert_eq!(answer.payload, [1]);
    assert!(host.gauge("memory_allocated_bytes") > 0);
    fs::remove_file(full).unwrap();
    host.until_room();
    assert_eq!(host.put(&mut c, key(448), None).1.tag, tag::ACCEPT);
}

#[test]
fn l0_10_cancel_eof_and_io_lifetime() {
    let base = base("store");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        4 * 1024 * 1024,
    );
    config.faults.delay_ms = 300;
    let mut host = StoreHost::open(config);
    let mut c = host.connect();
    let (op, _) = host.put(&mut c, key(128), None);
    let (header, bytes, hashes) = content_for(&c.descriptor, key(128), None, 0);
    assert!(matches!(
        host.store_mut().put_frame(
            c.id,
            op,
            PutFrameInput::Window {
                index: 0,
                pages: hashes,
            },
        ),
        PutFrameOutcome::Ok
    ));
    let mut outcome = host.store_mut().put_frame(
        c.id,
        op,
        PutFrameInput::Data {
            offset: header.page_segment,
            data: &bytes[header.page_segment as usize..header.row_segment as usize],
        },
    );
    while matches!(outcome, PutFrameOutcome::Blocked) {
        host.pump();
        std::thread::sleep(Duration::from_millis(2));
        outcome = host.store_mut().put_frame(
            c.id,
            op,
            PutFrameInput::Data {
                offset: header.page_segment,
                data: &bytes[header.page_segment as usize..header.row_segment as usize],
            },
        );
    }
    host.wait(|host| host.gauge("staging_allocated_bytes") > 0);
    host.cancel(&c, op);
    host.wait(|host| host.gauge("open_ops") == 0);
    assert!(
        host.gauge("staging_allocated_bytes") > 0,
        "I/O must retain extent after Cancel"
    );
    host.wait(|host| host.gauge("staging_allocated_bytes") == 0);
    host.wait(|host| host.gauge("nvme_reserved_bytes") == 0);
    host.close(&c);
    let mut c = host.connect();
    host.store_record(&mut c, key(128), None);
    let (_, answer) = host.put(&mut c, key(256), Some(key(128)));
    assert_eq!(answer.tag, tag::ACCEPT);
    host.close(&c);
    host.wait(|host| host.gauge("open_ops") == 0 && host.gauge("cleanup_records") == 0);
    let mut c = host.connect();
    host.fetch(&mut c, &[key(128)], 0, 2);
    host.wait(|host| host.gauge("leases") == 0);
    let mut blocked = host.connect();
    let (op, accept) = host.put(&mut blocked, key(640 * 64), None);
    assert_eq!(accept.tag, tag::ACCEPT);
    let (header, bytes, hashes) = content_for(&blocked.descriptor, key(640 * 64), None, 0);
    host.upload(&blocked, op, &header, &bytes, &hashes).unwrap();
    host.wait(|host| host.gauge("staging_allocated_bytes") > 0);
    let start = Instant::now();
    host.fetch(&mut c, &[key(128)], 0, 2);
    assert!(start.elapsed() < Duration::from_millis(500));
    host.cancel(&blocked, op);
    host.wait(|host| host.gauge("open_ops") == 0);
    host.wait(|host| host.gauge("staging_allocated_bytes") == 0);
    assert!(!host.entries().contains_key(&key(640 * 64)));
}

#[test]
fn l0_11_snapshot_deltas_resync_and_namespace() {
    let mut host = StoreHost::new(1024 * 1024, 4 * 1024 * 1024);
    let mut a = host.connect();
    host.store_record(&mut a, key(128), None);
    host.store_record(&mut a, key(256), Some(key(128)));
    assert_eq!(host.snapshot_keys(), [key(128), key(256)]);
    host.store_record(&mut a, key(384), Some(key(256)));
    assert_eq!(host.snapshot_keys(), [key(128), key(256), key(384)]);
    let (old_seq, _) = host.snapshot();
    host.store_record(&mut a, key(512), Some(key(384)));
    let (seq, entries) = host.snapshot();
    assert!(seq > old_seq);
    assert_eq!(entries.len(), 4);
    let mut other = descriptor();
    other.fingerprint = [9; 16];
    let other = host.connect_with(other);
    let empty = host
        .store()
        .snapshot(other.descriptor.namespace_id())
        .unwrap()
        .1;
    assert!(empty.is_empty());
    let mut wrong = descriptor();
    wrong.ranks[0].row_bytes += 1;
    assert!(host.try_connect_with(wrong).is_err());
}

#[test]
fn l0_12_round_robin_and_bounded_dead_client_cleanup() {
    let mut host = Host::new(8 * 1024 * 1024, 64 * 1024 * 1024);
    host.start();
    let mut a = host.client();
    let mut previous = None;
    for i in 1..=160 {
        let k = key(i * 64);
        a.store(k, previous);
        previous = Some(k);
    }
    host.durable(160);
    let mut b = host.client();
    let keys: Vec<_> = (1..=160).map(|i| key(i * 64)).collect();
    for _ in 0..4 {
        a.get_start(&keys, 0, 160);
    }
    drop(a);
    let start = Instant::now();
    b.fetch(&[key(64)], 0, 1);
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "live Get blocked for {elapsed:?}"
    );
    wait(|| host.gauge("leases") == 0 && host.gauge("cleanup_records") == 0);
    // One flooding producer is limited to one frame per turn and four admitted operations.
    let mut flood = host.client();
    let join = std::thread::spawn(move || {
        for _ in 0..2000 {
            flood.send(tag::DATA, 99999, vec![0; 4096]);
        }
    });
    let start = Instant::now();
    b.fetch(&[key(64)], 0, 1);
    assert!(start.elapsed() < Duration::from_millis(500));
    join.join().unwrap();
}

#[test]
fn l0_13_unknown_finished_cancelled_and_stale_frames() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut c = host.client();
    c.store(key(128), None);
    let finished = c.next_op - 1;
    c.send(tag::DATA, finished, vec![0; 4096]);
    c.send(tag::DATA, 99999, vec![0; 4096]);
    Frame {
        tag: tag::DATA,
        generation: c.generation ^ 123,
        op: finished,
        payload: vec![0; 4096],
    }
    .write(&mut c.stream)
    .unwrap();
    let (cancelled, _) = c.put(key(256), None);
    c.send(tag::CANCEL, cancelled, Vec::new());
    c.send(tag::DATA, cancelled, vec![0; 4096]);
    wait(|| host.gauge("unknown_frames_total") == 4);
    c.fetch(&[key(128)], 0, 2);
    host.durable(1);
    let mut malformed = host.client();
    use std::io::{Read, Write};
    malformed
        .stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    malformed.stream.write_all(&0u32.to_le_bytes()).unwrap();
    assert_eq!(malformed.stream.read(&mut [0]).unwrap(), 0);
}

#[test]
fn l0_14_shm_and_real_nvme_throughput() {
    support::throughput::run(true);
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
fn l0_16_dispatch_prioritises_gets() {
    let mut host = StoreHost::new(1024 * 1024, 64 * 1024 * 1024);
    let mut c = host.connect();
    host.store_record(&mut c, key(128), None);
    for _ in 0..4 {
        host.store_mut().submit_get(
            c.id,
            c.next_op,
            store::GetRequest {
                keys: vec![key(128)],
                skip_pages: 0,
                expected_pages: 2,
                row: true,
            },
        );
        c.next_op += 1;
    }
    for tokens in [256, 384, 512, 640] {
        let op = c.next_op;
        c.next_op += 1;
        host.store_mut().submit_put(
            c.id,
            op,
            store::PutRequest {
                key: key(tokens),
                published_parent: None,
                bucket: [4; 16],
                chain_pages: (tokens / 64) as u32,
                published_tail_pages: (tokens / 64) as u32,
                page_bytes: c.descriptor.page_bytes(),
                row_bytes: c.descriptor.row_bytes(),
                row: true,
            },
        );
    }
    let outcomes = host.store_mut().dispatch();
    assert_eq!(outcomes.len(), 5);
    for outcome in &outcomes[..4] {
        assert!(matches!(outcome, DispatchOutcome::Get { .. }));
    }
    assert!(matches!(outcomes[4], DispatchOutcome::Put { .. }));
    // A saturated Put queue never delays a Get by more than four Put slots.
    for tokens in [768, 896, 1024, 1152, 1280] {
        let op = c.next_op;
        c.next_op += 1;
        host.store_mut().submit_put(
            c.id,
            op,
            store::PutRequest {
                key: key(tokens),
                published_parent: None,
                bucket: [4; 16],
                chain_pages: (tokens / 64) as u32,
                published_tail_pages: (tokens / 64) as u32,
                page_bytes: c.descriptor.page_bytes(),
                row_bytes: c.descriptor.row_bytes(),
                row: true,
            },
        );
    }
    let get_op = c.next_op;
    c.next_op += 1;
    host.store_mut().submit_get(
        c.id,
        get_op,
        store::GetRequest {
            keys: vec![key(128)],
            skip_pages: 0,
            expected_pages: 2,
            row: true,
        },
    );
    let outcomes = host.store_mut().dispatch();
    assert!(matches!(outcomes[0], DispatchOutcome::Get { .. }));
}

#[test]
fn l0_17_disk_queue_read_write_interleave() {
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
    // Queue the writes first so a write is pending when the reads arrive.
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

#[test]
fn l0_18_write_budget() {
    let base = base("budget");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        64 * 1024 * 1024,
        64 * 1024 * 1024,
    );
    config.write_budget = 1024 * 1024;
    let mut host = StoreHost::open(config);
    let mut a = host.connect();
    host.store_record(&mut a, key(128), None);
    host.store_record(&mut a, key(2048 * 64), None);
    assert!(host.store().budget_balance(a.id).unwrap() < 0);
    // Gets are never throttled by the write budget.
    host.fetch(&mut a, &[key(128)], 0, 2);
    let (_, answer) = host.put(&mut a, key(256), None);
    assert_eq!(answer.tag, tag::RETRY);
    assert_eq!(answer.payload, [1]);
    host.pump();
    assert!(
        !host
            .events
            .iter()
            .any(|event| matches!(event, store::StoreEvent::Room { .. }))
    );
    host.store_mut()
        .tick_at(Instant::now() + Duration::from_secs(8));
    host.collect();
    assert!(
        host.events
            .iter()
            .any(|event| matches!(event, store::StoreEvent::Room { .. }))
    );
    assert_eq!(host.put(&mut a, key(256), None).1.tag, tag::ACCEPT);
    // Buckets are independent per connection.
    let mut b = host.connect();
    assert_eq!(host.put(&mut b, key(384), None).1.tag, tag::ACCEPT);
    // 0 means unlimited.
    let mut unlimited = StoreHost::new(64 * 1024 * 1024, 64 * 1024 * 1024);
    let mut c = unlimited.connect();
    unlimited.store_record(&mut c, key(2048 * 64), None);
    unlimited.store_record(&mut c, key(2048 * 64 + 64), Some(key(2048 * 64)));
}

#[test]
fn l0_19_v2_descriptor_format_and_v3_refusal() {
    let mut host = StoreHost::new(1024 * 1024, 1024 * 1024);
    let mut c = host.connect();
    host.store_record(&mut c, key(128), None);
    let directory = host.config.root.join(c.descriptor.name());
    let bytes = fs::read(directory.join("descriptor")).unwrap();
    assert_eq!(&bytes[..8], b"FLCDSC2\0");
    let length = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    assert_eq!(bytes.len(), 16 + length);
    assert_eq!(
        disk::crc32(&bytes[..12 + length]),
        u32::from_le_bytes(bytes[12 + length..].try_into().unwrap())
    );
    host.close_store();
    let root = host.config.root.clone();
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

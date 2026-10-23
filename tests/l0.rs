mod support;
use flare_cache_manager::{
    arena::Arena,
    disk,
    protocol::{self, Frame, tag},
    record::{self},
    server,
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
    let mut host = Host::new(65536, 49152);
    host.start();
    let mut c = host.client();
    c.store(key(128), None);
    c.store(key(256), Some(key(128)));
    host.durable(2);
    let (mut op, mut answer) = c.put(key(512), None);
    while answer.tag == tag::RETRY {
        c.until(tag::ROOM, 1);
        (op, answer) = c.put(key(512), None);
    }
    assert_eq!(answer.tag, tag::ACCEPT);
    c.send(tag::CANCEL, op, Vec::new());
    c.resync();
    assert!(!c.entries.contains_key(&key(256)));
}

#[test]
fn l0_03_accept_shapes_and_compacted_replacement() {
    let mut host = Host::new(1024 * 1024, 98304);
    host.start();
    let mut c = host.client();
    let (op, answer) = c.put(key(448), Some(key(384)));
    assert_eq!(answer.tag, tag::ACCEPT);
    assert_eq!(answer.payload, [0, 0, 0, 0, 0]);
    c.send(tag::CANCEL, op, Vec::new());
    c.store(key(128), None);
    c.store(key(256), Some(key(128)));
    host.durable(2);
    // Pin the leaf with a live accepted child, forcing its single-child ancestor to compact.
    let (child, answer) = c.put(key(320), Some(key(256)));
    assert_eq!(answer.tag, tag::ACCEPT);
    let (_, answer) = c.put(key(640), None);
    assert_eq!(answer.tag, tag::RETRY);
    wait(|| host.gauge("compactions_total") > 0);
    std::thread::sleep(Duration::from_millis(20));
    c.resync();
    assert!(!c.entries[&key(128)].row);
    let (op, answer) = c.put(key(128), None);
    assert_eq!(answer.tag, tag::ACCEPT);
    let (header, bytes, hashes) = c.content(key(128), None, 0);
    c.upload(op, &header, &bytes, &hashes);
    assert_eq!(c.until(tag::STORED, op).payload, [0]);
    assert!(c.entries[&key(128)].row);
    assert_eq!(c.entries[&key(256)].parent, Some(key(128)));
    c.send(tag::CANCEL, child, Vec::new());
}

#[test]
fn l0_04_present_and_inflight_dedup() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut a = host.client();
    let mut b = host.client();
    let (op, answer) = a.put(key(128), None);
    assert_eq!(answer.tag, tag::ACCEPT);
    let (_, answer) = b.put(key(128), None);
    assert_eq!(answer.tag, tag::RETRY);
    assert_eq!(answer.payload, [2]);
    let (header, bytes, hashes) = a.content(key(128), None, 0);
    a.upload(op, &header, &bytes, &hashes);
    assert_eq!(a.until(tag::STORED, op).payload, [0]);
    assert_eq!(b.put(key(128), None).1.tag, tag::PRESENT);
    // Four open puts per direction, independent of key dedup.
    for tokens in [256, 384, 512, 640] {
        assert_eq!(a.put(key(tokens), None).1.tag, tag::ACCEPT);
    }
    let answer = a.put(key(768), None).1;
    assert_eq!(answer.tag, tag::RETRY);
    assert_eq!(answer.payload, [0]);
    drop(a);
    wait(|| host.gauge("open_ops") == 0);
    let mut c = host.client();
    c.store(key(3), None);
    c.store(key(5), Some(key(3)));
    assert_eq!(c.entries[&key(5)].pages, 0);
    c.fetch(&[key(3), key(5)], 0, 0);
}

#[test]
fn l0_05_invalid_checksum_short_payload_and_offsets() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut c = host.client();
    for (index, tokens) in [128, 256, 384, 512, 640, 768].into_iter().enumerate() {
        let k = key(tokens);
        let (op, answer) = c.put(k, None);
        assert_eq!(answer.tag, tag::ACCEPT);
        let (header, mut bytes, hashes) = c.content(k, None, 0);
        if index == 0 {
            bytes[header.page_segment as usize] ^= 1;
            c.upload(op, &header, &bytes, &hashes);
        } else if index == 1 {
            c.send(
                tag::COMMIT,
                op,
                bytes[..header.page_segment as usize].to_vec(),
            );
        } else if index == 2 {
            let mut p = protocol::Encoder::default();
            p.u32(0);
            p.u32(1);
            p.u64(hashes[0]);
            c.send(tag::WINDOW, op, p.0);
            c.data(
                op,
                header.page_segment + 4096,
                &bytes[header.page_segment as usize..header.page_segment as usize + 4096],
            );
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
            c.upload(op, &header, &bytes, &hashes);
        }
        assert_eq!(c.until(tag::STORED, op).payload[0], 1);
        c.resync();
        assert!(!c.entries.contains_key(&k));
    }
    wait(|| host.gauge("nvme_reserved_bytes") == 0 && host.gauge("memory_allocated_bytes") == 0);
}

#[test]
fn l0_06_parent_first_chain_closed_durability() {
    let mut host = Host::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    host.config.faults.pause_at = Some("parent_write".into());
    let signal = host.base.join("gate");
    host.config.faults.signal = Some(signal.clone());
    host.start();
    let mut c = host.client();
    c.store(key(128), None);
    wait(|| signal.exists());
    c.store(key(256), Some(key(128)));
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
    wait(|| signal.exists());
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
    c.resync();
    assert!(c.entries.values().all(|e| e.durable));
}

#[test]
fn l0_07_multiprocess_kill_recovery() {
    if let Ok(base) = std::env::var("CM_CHILD_BASE") {
        let base = std::path::PathBuf::from(base);
        let mut config = server::Config::new(
            base.join("records"),
            base.join("run/m.sock"),
            4 * 1024 * 1024,
            64 * 1024 * 1024,
        );
        config.faults.pause_at = std::env::var("CM_CHILD_PAUSE").ok();
        config.faults.signal = Some(base.join("gate"));
        server::run(config, Arc::new(std::sync::atomic::AtomicBool::new(false))).unwrap();
        return;
    }
    for point in ["data", "before_rename", "after_rename", "parent_write"] {
        let base = base(point);
        let config = server::Config::new(
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
        wait(|| config.socket.exists());
        let mut c = Client::connect(&config, descriptor());
        if point == "data" {
            let (op, answer) = c.put(key(128), None);
            assert_eq!(answer.tag, tag::ACCEPT);
            let (header, bytes, hashes) = c.content(key(128), None, 0);
            let mut p = protocol::Encoder::default();
            p.u32(0);
            p.u32(2);
            for hash in hashes {
                p.u64(hash);
            }
            c.send(tag::WINDOW, op, p.0);
            c.data(
                op,
                header.page_segment,
                &bytes[header.page_segment as usize..header.page_segment as usize + 4096],
            );
        } else {
            c.store(key(128), None);
            if point == "parent_write" {
                c.store(key(256), Some(key(128)));
            }
            wait(|| base.join("gate").exists());
        }
        wait(|| base.join("gate").exists());
        child.kill().unwrap();
        let status = child.wait().unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        drop(c);
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
        let mut restarted = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "l0_07_multiprocess_kill_recovery", "--nocapture"])
            .env("CM_CHILD_BASE", &base)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        wait(|| std::os::unix::net::UnixStream::connect(&config.socket).is_ok());
        // The stale socket can briefly exist before bind; retry connection.
        std::thread::sleep(Duration::from_millis(20));
        let c = Client::connect(&config, descriptor());
        assert_eq!(c.entries.len(), keys.len());
        restarted.kill().unwrap();
        restarted.wait().unwrap();
        fs::remove_dir_all(base).unwrap();
    }
}

#[test]
fn l0_08_streaming_and_nvme_oversize() {
    let mut host = Host::new(65536, 2 * 1024 * 1024);
    host.start();
    let mut c = host.client();
    let bytes = c.store(key(128 * 64), None);
    host.durable(1);
    assert_eq!(host.gauge("memory_allocated_bytes"), 0);
    let data = c.fetch(&[key(128 * 64)], 0, 128);
    let mut actual = vec![0; bytes.len()];
    for (_, offset, chunk) in data {
        actual[offset as usize..offset as usize + chunk.len()].copy_from_slice(&chunk);
    }
    assert_eq!(actual, bytes);
    assert_eq!(c.put(key(1024 * 64), None).1.tag, tag::OVERSIZE);
    host.stop();
    host.start();
    let mut c = host.client();
    assert_eq!(c.entries.len(), 1);
    c.fetch(&[key(128 * 64)], 64, 64);
    drop(c);
    let mut refill = Host::new(65536, 1024 * 1024);
    refill.start();
    refill.client().store(key(128), None);
    refill.durable(1);
    refill.stop();
    refill.start();
    let mut c = refill.client();
    assert_eq!(c.entries[&key(128)].tier, 1);
    c.fetch(&[key(128)], 0, 2);
    wait(|| refill.gauge("memory_allocated_bytes") > 0);
    c.resync();
    assert_eq!(c.entries[&key(128)].tier, 0);
    let mut split = Host::new(4096, 32 * 1024 * 1024);
    split.start();
    let mut geometry = descriptor();
    geometry.ranks[0].planes = vec![protocol::MAX_DATA as u64 + 4096];
    let mut c = Client::connect(&split.config, geometry);
    let expected = c.store(key(128), None);
    let mut actual = vec![0; expected.len()];
    for (_, offset, data) in c.fetch(&[key(128)], 0, 2) {
        actual[offset as usize..offset as usize + data.len()].copy_from_slice(&data);
    }
    assert_eq!(actual, expected);
}

#[test]
fn l0_09_disk_full_budget_and_room() {
    let mut host = Host::new(65536, 1024 * 1024);
    let full = host.base.join("full");
    fs::create_dir_all(&host.base).unwrap();
    fs::write(&full, b"full").unwrap();
    host.config.faults.disk_full = Some(full.clone());
    host.start();
    let mut c = host.client();
    c.store(key(384), None);
    wait(|| host.gauge("nvme_errors_total") > 0);
    let (_, answer) = c.put(key(448), None);
    assert_eq!(answer.tag, tag::RETRY);
    assert_eq!(answer.payload, [1]);
    assert!(host.gauge("memory_allocated_bytes") > 0);
    fs::remove_file(full).unwrap();
    c.until(tag::ROOM, 1);
    assert_eq!(c.put(key(448), None).1.tag, tag::ACCEPT);
}

#[test]
fn l0_10_cancel_eof_and_io_lifetime() {
    if let Ok(socket) = std::env::var("CM_CLIENT_SOCKET") {
        let config = server::Config::new(std::path::PathBuf::new(), socket.into(), 4096, 1048576);
        let mut client = Client::connect(&config, descriptor());
        assert_eq!(client.put(key(256), Some(key(128))).1.tag, tag::ACCEPT);
        fs::write(std::env::var("CM_CLIENT_READY").unwrap(), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }
    let mut host = Host::new(4096, 4 * 1024 * 1024);
    host.config.faults.delay_ms = 300;
    host.start();
    let mut c = host.client();
    let (op, _) = c.put(key(128), None);
    let (header, bytes, hashes) = c.content(key(128), None, 0);
    let mut p = protocol::Encoder::default();
    p.u32(0);
    p.u32(2);
    for hash in hashes {
        p.u64(hash);
    }
    c.send(tag::WINDOW, op, p.0);
    c.data(
        op,
        header.page_segment,
        &bytes[header.page_segment as usize..header.row_segment as usize],
    );
    wait(|| host.gauge("staging_allocated_bytes") > 0);
    c.send(tag::CANCEL, op, Vec::new());
    wait(|| host.gauge("open_ops") == 0);
    assert!(
        host.gauge("staging_allocated_bytes") > 0,
        "I/O must retain extent after Cancel"
    );
    wait(|| host.gauge("staging_allocated_bytes") == 0);
    wait(|| host.gauge("nvme_reserved_bytes") == 0);
    drop(c);
    let mut c = host.client();
    c.store(key(128), None);
    let (_, answer) = c.put(key(256), Some(key(128)));
    assert_eq!(answer.tag, tag::ACCEPT);
    drop(c);
    wait(|| host.gauge("open_ops") == 0 && host.gauge("cleanup_records") == 0);
    let mut c = host.client();
    c.fetch(&[key(128)], 0, 2);
    wait(|| host.gauge("leases") == 0);
    let ready = host.base.join("client-ready");
    let mut replica = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "l0_10_cancel_eof_and_io_lifetime", "--nocapture"])
        .env("CM_CLIENT_SOCKET", &host.config.socket)
        .env("CM_CLIENT_READY", &ready)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    wait(|| ready.exists());
    replica.kill().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(replica.wait().unwrap().signal(), Some(libc::SIGKILL));
    wait(|| host.gauge("open_ops") == 0 && host.gauge("cleanup_records") == 0);
    c.fetch(&[key(128)], 0, 2);
}

#[test]
fn l0_11_snapshot_deltas_resync_and_namespace() {
    let mut host = Host::new(1024 * 1024, 4 * 1024 * 1024);
    host.start();
    let mut a = host.client();
    a.store(key(128), None);
    a.store(key(256), Some(key(128)));
    let mut b = host.client();
    assert_eq!(b.snapshots[0], [key(128), key(256)]);
    a.store(key(384), Some(key(256)));
    b.resync();
    assert_eq!(b.snapshots.last().unwrap(), &[key(128), key(256), key(384)]);
    let old_seq = b.seq;
    b.seq = old_seq.saturating_sub(1);
    a.store(key(512), Some(key(384)));
    loop {
        let frame = b.read();
        if frame.tag == tag::SNAPSHOT_END {
            break;
        }
    }
    assert!(b.seq > old_seq);
    assert_eq!(b.entries.len(), 4);
    let mut other = descriptor();
    other.fingerprint = [9; 16];
    let other = Client::connect(&host.config, other);
    assert!(other.entries.is_empty());
    let mut wrong = descriptor();
    wrong.ranks[0].row_bytes += 1;
    let mut stream = std::os::unix::net::UnixStream::connect(&host.config.socket).unwrap();
    let mut p = protocol::Encoder::default();
    p.u32(1);
    p.text("wrong");
    p.u32(std::process::id());
    p.descriptor(&wrong);
    Frame {
        tag: tag::HELLO,
        generation: 0,
        op: 1,
        payload: p.0,
    }
    .write(&mut stream)
    .unwrap();
    let reject = Frame::read(&mut stream).unwrap();
    assert_eq!(reject.tag, tag::REJECT);
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
    let scratch =
        std::env::var_os("CM_NVME_SCRATCH").expect("CM_NVME_SCRATCH must name an NVMe scratch directory");
    for (tier, root) in [
        ("shm", std::path::PathBuf::from("/dev/shm")),
        ("nvme", scratch.into()),
    ] {
        let mut host = Host::new(4096, 256 * 1024 * 1024);
        host.config.root = root.join(format!("cm-throughput-{}", std::process::id()));
        host.start();
        let mut c = host.client();
        let k = key(4096 * 64);
        let start = Instant::now();
        let bytes = c.store(k, None);
        host.durable(1);
        let put_seconds = start.elapsed().as_secs_f64();
        let start = Instant::now();
        let data = c.fetch(&[k], 0, 4096);
        let get_seconds = start.elapsed().as_secs_f64();
        let mut actual = vec![0; bytes.len()];
        for (_, offset, chunk) in data {
            actual[offset as usize..offset as usize + chunk.len()].copy_from_slice(&chunk);
        }
        assert_eq!(actual, bytes);
        let size = bytes.len();
        println!(
            "L0 throughput {tier}: bytes={size} put_seconds={put_seconds:.6} get_seconds={get_seconds:.6} put_MiB_s={:.2} get_MiB_s={:.2}",
            size as f64 / 1048576.0 / put_seconds,
            size as f64 / 1048576.0 / get_seconds
        );
        assert!(
            size as f64 / put_seconds > 5.0 * 1048576.0,
            "Put throughput regression"
        );
        assert!(
            size as f64 / get_seconds > 5.0 * 1048576.0,
            "Get throughput regression"
        );
    }
}

#[path = "../tests/support/mod.rs"]
mod support;
use flare_cache_manager::{
    protocol::{Decoder, tag},
    record,
};
use std::time::Instant;
use support::*;

fn main() {
    const PAGES: u32 = 8960;
    for (mode, memory, root) in [
        (
            "dram-shm",
            1024 * 1024 * 1024,
            std::path::PathBuf::from("/dev/shm"),
        ),
        ("stream-shm", 4096, std::path::PathBuf::from("/dev/shm")),
        (
            "stream-nvme",
            4096,
            std::env::var_os("CM_NVME_SCRATCH").unwrap().into(),
        ),
    ] {
        let mut host = Host::new(memory, 2 * 1024 * 1024 * 1024);
        host.config.root = root.join(format!("cm-bench-{}-{mode}", std::process::id()));
        host.start();
        let mut desc = descriptor();
        desc.ranks[0].planes = vec![65536];
        let mut c = Client::connect(&host.config, desc);
        let k = key(u64::from(PAGES) * 64);
        let prep = Instant::now();
        let (header, bytes, hashes) = c.content(k, None, 0);
        let prep = prep.elapsed().as_secs_f64();
        let mut actual = vec![1; bytes.len()];
        let start = Instant::now();
        let (op, answer) = c.put(k, None);
        assert_eq!(answer.tag, tag::ACCEPT);
        c.upload(op, &header, &bytes, &hashes);
        assert_eq!(c.until(tag::STORED, op).payload, [0]);
        let put = start.elapsed().as_secs_f64();
        host.durable(1);
        let mut frame = flare_cache_manager::protocol::Frame {
            tag: 0,
            generation: 0,
            op: 0,
            payload: vec![0; 4 * 1024 * 1024 + 12],
        };
        let start = Instant::now();
        let op = c.get_start(&[k], 0, PAGES);
        loop {
            frame.read_into(&mut c.stream).unwrap();
            if frame.op != op {
                continue;
            }
            if frame.tag == tag::DONE {
                assert_eq!(frame.payload, [0]);
                break;
            }
            assert_eq!(frame.tag, tag::DATA);
            let mut d = Decoder::new(&frame.payload);
            assert_eq!(d.u32().unwrap(), 0);
            let offset = d.u64().unwrap() as usize;
            let data = &frame.payload[12..];
            actual[offset..offset + data.len()].copy_from_slice(data);
        }
        let get = start.elapsed().as_secs_f64();
        assert_eq!(actual, bytes);
        for (page, hash) in hashes.iter().enumerate() {
            let start = header.page_segment as usize + page * 65536;
            assert_eq!(record::checksum(&actual[start..start + 65536]), *hash);
        }
        println!(
            "{mode} bytes={} prepare_s={prep:.6} put_s={put:.6} get_s={get:.6} put_GB_s={:.3} get_GB_s={:.3}",
            bytes.len(),
            bytes.len() as f64 / put / 1e9,
            bytes.len() as f64 / get / 1e9
        );
    }
}

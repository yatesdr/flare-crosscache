use super::*;

struct Manager(std::process::Child);
impl Drop for Manager {
    fn drop(&mut self) {
        let _ = self.0.kill();
        self.0.wait().unwrap();
    }
}

pub fn run(enforce: bool) {
    if let Some(root) = std::env::var_os("CM_BENCH_ROOT") {
        let mut config = Config::new(
            root.into(),
            std::env::var_os("CM_BENCH_SOCKET").unwrap().into(),
            std::env::var("CM_BENCH_MEMORY").unwrap().parse().unwrap(),
            2 << 30,
        );
        config.metrics = Some(std::env::var("CM_BENCH_METRICS").unwrap());
        server::run(config, Arc::new(AtomicBool::new(false))).unwrap();
        return;
    }
    const PAGES: u32 = 8960;
    let scratch = std::env::var_os("CM_NVME_SCRATCH").expect("CM_NVME_SCRATCH is required");
    for (mode, memory, root) in [
        ("dram-shm", 1 << 30, PathBuf::from("/dev/shm")),
        ("stream-shm", 4096, PathBuf::from("/dev/shm")),
        ("stream-nvme", 4096, scratch.into()),
    ] {
        let mut host = Host::new(memory, 2 << 30);
        host.config.root = root.join(format!("cm-throughput-{}-{mode}", std::process::id()));
        let _manager = Manager(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "l0_14_shm_and_real_nvme_throughput",
                    "--nocapture",
                ])
                .env("CM_BENCH_ROOT", &host.config.root)
                .env("CM_BENCH_SOCKET", &host.config.socket)
                .env("CM_BENCH_MEMORY", memory.to_string())
                .env("CM_BENCH_METRICS", host.config.metrics.as_ref().unwrap())
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        wait(|| host.config.socket.exists());
        let mut desc = descriptor();
        desc.ranks[0].planes = vec![65536];
        let mut client = Client::connect(&host.config, desc);
        let mut key = key(u64::from(PAGES) * 64);
        let prepare = Instant::now();
        let (mut header, mut bytes, hashes) = client.content(key, None, 0);
        let mut actual = vec![1; bytes.len()];
        println!(
            "L0 {mode} bytes={} pages={PAGES} page_bytes=65536 frame_data_max={} prepare_s={:.6}",
            bytes.len(),
            protocol::MAX_DATA,
            prepare.elapsed().as_secs_f64()
        );
        let mut puts = Vec::new();
        let mut gets = Vec::new();
        for sample in 0..3 {
            key.digest[0] = sample;
            header.key = key;
            bytes[..4096].copy_from_slice(&encode_header(&header));
            let start = Instant::now();
            let (op, accept) = client.put(key, None);
            assert_eq!(accept.tag, tag::ACCEPT);
            client.upload(op, &header, &bytes, &hashes);
            assert_eq!(client.until(tag::STORED, op).payload, [0]);
            let put = start.elapsed().as_secs_f64();
            while !client.entries[&key].durable {
                client.read();
            }
            let durable = start.elapsed().as_secs_f64();
            let start = Instant::now();
            let op = client.get_start(&[key], 0, PAGES);
            let mut cursor = 0;
            loop {
                let (mut frame, length) = Frame::read_header(&mut client.stream).unwrap();
                assert_eq!(frame.generation, client.generation);
                if frame.tag != tag::DATA {
                    frame.payload.resize(length - 17, 0);
                    client.stream.read_exact(&mut frame.payload).unwrap();
                    client.apply(&frame);
                    if frame.tag == tag::DONE && frame.op == op {
                        assert_eq!(frame.payload, [0]);
                        break;
                    }
                    continue;
                }
                assert_eq!(frame.op, op);
                let mut prefix = [0; 12];
                client.stream.read_exact(&mut prefix).unwrap();
                let mut decoder = Decoder::new(&prefix);
                assert_eq!(decoder.u32().unwrap(), 0);
                let offset = decoder.u64().unwrap() as usize;
                assert_eq!(offset, cursor);
                let data = &mut actual[offset..offset + length - 29];
                client.stream.read_exact(data).unwrap();
                if offset >= header.page_segment as usize && offset < header.row_segment as usize {
                    let page = (offset - header.page_segment as usize) / 65536;
                    for (index, bytes) in data.chunks_exact(65536).enumerate() {
                        assert_eq!(record::checksum(bytes), hashes[page + index]);
                    }
                } else if offset == header.row_segment as usize {
                    assert_eq!(record::checksum(data), header.row_checksum);
                }
                cursor += data.len();
            }
            let get = start.elapsed().as_secs_f64();
            assert_eq!(cursor, bytes.len());
            assert_eq!(actual, bytes);
            let put_rate = bytes.len() as f64 / put / 1e9;
            let get_rate = bytes.len() as f64 / get / 1e9;
            puts.push(put_rate);
            gets.push(get_rate);
            println!(
                "L0 {mode} sample={sample} put_s={put:.6} durable_s={durable:.6} get_s={get:.6} put_GB_s={put_rate:.3} get_GB_s={get_rate:.3}"
            );
            wait(|| host.gauge("leases") == 0);
        }
        puts.sort_by(f64::total_cmp);
        gets.sort_by(f64::total_cmp);
        println!(
            "L0 {mode} median_put_GB_s={:.3} median_get_GB_s={:.3}",
            puts[1], gets[1]
        );
        if enforce && mode == "dram-shm" {
            assert!(puts[1] >= 3.0, "DRAM Put below 3 GB/s: {puts:?}");
            assert!(gets[1] >= 3.0, "DRAM Get below 3 GB/s: {gets:?}");
        }
    }
}

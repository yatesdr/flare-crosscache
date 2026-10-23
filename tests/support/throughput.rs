use super::*;
use std::time::Duration;

fn cpu_seconds() -> f64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
    }
    usage.ru_utime.tv_sec as f64
        + usage.ru_utime.tv_usec as f64 / 1e6
        + usage.ru_stime.tv_sec as f64
        + usage.ru_stime.tv_usec as f64 / 1e6
}

fn p99(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let index = ((values.len() as f64) * 0.99).ceil() as usize;
    values[index.saturating_sub(1).min(values.len() - 1)]
}

pub fn run(enforce: bool) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(benchmark(enforce));
}

async fn benchmark(enforce: bool) {
    const PAGES: u32 = 128;
    let scratch = std::env::var_os("CM_NVME_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/dev/shm"));
    for (mode, memory, root) in [
        ("dram-shm", 1usize << 30, PathBuf::from("/dev/shm")),
        ("stream-shm", 4096, PathBuf::from("/dev/shm")),
        ("stream-nvme", 4096, scratch.clone()),
    ] {
        let base = base("bench");
        let mut config = Config::new(
            root.join(format!("cm-bench-{}-{mode}", std::process::id())),
            base.join("run/m.sock"),
            memory,
            2u64 << 30,
        );
        config.name = instance_name("bench");
        let mut host = Host::open_config(config);
        host.start();
        let mut descriptor = descriptor();
        descriptor.ranks[0].planes = vec![65536];
        let mut client = Client::connect(&host.config, descriptor).await.unwrap();
        let key = key(u64::from(PAGES) * 64);
        let (_, bytes, _) = client.content(key, None, 0);
        println!(
            "L0 {mode} bytes={} pages={PAGES} page_bytes=65536",
            bytes.len()
        );
        let mut puts = Vec::new();
        let mut gets = Vec::new();
        let mut polls = Vec::new();
        for sample in 0..6u8 {
            let mut sample_key = key;
            sample_key.digest[0] = sample;
            let started = Instant::now();
            let value = client.store(sample_key, None).await;
            let put = started.elapsed().as_secs_f64();
            let started = Instant::now();
            let actual = client.fetch(&[sample_key], 0, PAGES).await;
            let get = started.elapsed().as_secs_f64();
            assert_eq!(actual[0], value);
            polls.push(get);
            if sample == 0 {
                continue; // warm-up
            }
            let put_rate = value.len() as f64 / put / 1e9;
            let get_rate = value.len() as f64 / get / 1e9;
            puts.push(put_rate);
            gets.push(get_rate);
            println!(
                "L0 {mode} sample={sample} put_s={put:.6} get_s={get:.6} put_GB_s={put_rate:.3} get_GB_s={get_rate:.3}"
            );
        }
        let cpu = cpu_seconds();
        puts.sort_by(f64::total_cmp);
        gets.sort_by(f64::total_cmp);
        let median_put = puts[puts.len() / 2];
        let median_get = gets[gets.len() / 2];
        let cpu_s_per_gb = cpu / ((bytes.len() as f64 * 10.0) / 1e9);
        println!(
            "L0 {mode} median_put_GB_s={median_put:.3} median_get_GB_s={median_get:.3} \
             cpu_s_per_GB={cpu_s_per_gb:.4} poll_p99_ms={:.3}",
            p99(&mut polls) * 1e3
        );
        if enforce && mode == "dram-shm" {
            assert!(median_put >= 1.0, "DRAM Put below 1 GB/s: {puts:?}");
            assert!(median_get >= 1.0, "DRAM Get below 1 GB/s: {gets:?}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

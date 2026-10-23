use flare_cache_manager::{
    Result, invalid,
    server::{self, Config},
};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
};

fn size(value: &str) -> Result<u64> {
    let (number, multiplier) = match value.as_bytes().last() {
        Some(b'K' | b'k') => (&value[..value.len() - 1], 1024),
        Some(b'M' | b'm') => (&value[..value.len() - 1], 1024 * 1024),
        Some(b'G' | b'g') => (&value[..value.len() - 1], 1024 * 1024 * 1024),
        Some(b'T' | b't') => (&value[..value.len() - 1], 1024u64.pow(4)),
        _ => (value, 1),
    };
    number
        .parse::<u64>()
        .ok()
        .and_then(|number| number.checked_mul(multiplier))
        .ok_or_else(|| invalid("invalid byte size"))
}
fn main() {
    if let Err(error) = start() {
        eprintln!("flare-cache-manager: {error}");
        std::process::exit(1);
    }
}
fn start() -> Result<()> {
    let mut config = Config::new(
        PathBuf::from("/var/lib/flare-cache"),
        PathBuf::from("/run/flare-cache/manager.sock"),
        1024 * 1024 * 1024,
        64 * 1024 * 1024 * 1024,
    );
    let mut args = std::env::args().skip(1);
    while let Some(option) = args.next() {
        if option == "--help" || option == "-h" {
            println!(
                "flare-cache-manager [--root DIR] [--socket PATH] [--memory BYTES] [--nvme BYTES] [--staging BYTES] [--metrics HOST:PORT]\nSizes accept binary K, M, G, T suffixes. Linux only; same uid and gid as clients."
            );
            return Ok(());
        }
        let value = args
            .next()
            .ok_or_else(|| invalid(format!("missing value for {option}")))?;
        match option.as_str() {
            "--root" => config.root = value.into(),
            "--socket" => config.socket = value.into(),
            "--memory" => {
                config.memory =
                    usize::try_from(size(&value)?).map_err(|_| invalid("memory size"))?
            }
            "--nvme" => config.nvme = size(&value)?,
            "--staging" => {
                config.staging =
                    usize::try_from(size(&value)?).map_err(|_| invalid("staging size"))?
            }
            "--metrics" => config.metrics = Some(value),
            _ => return Err(invalid(format!("unknown option {option}"))),
        }
    }
    server::run(config, Arc::new(AtomicBool::new(false)))
}

use flare_crosscache::{Result, control, invalid, store::Config};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
};

fn size(option: &str, value: &str) -> Result<u64> {
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
        .ok_or_else(|| invalid(format!("--{option}: invalid byte size '{value}'")))
}

fn parse_name(value: &str) -> Result<String> {
    let valid = !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if !valid {
        return Err(invalid("--name must match ^[A-Za-z0-9._-]{1,64}$"));
    }
    Ok(value.to_owned())
}

fn main() {
    if let Err(error) = start() {
        eprintln!("flare-crosscache: {error}");
        std::process::exit(1);
    }
}

fn start() -> Result<()> {
    let mut name = "default".to_owned();
    let mut socket: Option<PathBuf> = None;
    let mut root: Option<PathBuf> = None;
    let mut memory: u64 = 1024 * 1024 * 1024;
    let mut nvme: Option<u64> = None;
    let mut staging: u64 = 256 * 1024 * 1024;
    let mut write_budget = 0u64;
    let mut metrics = None;
    let mut args = std::env::args().skip(1);
    while let Some(option) = args.next() {
        if option == "--help" || option == "-h" {
            println!(
                "flare-crosscache [--name NAME] [--socket PATH] [--root PATH] [--memory BYTES] \
                 [--nvme BYTES] [--staging BYTES] [--write-budget BYTES/s] [--metrics HOST:PORT]\n\
                 Sizes accept binary K, M, G, T suffixes. Linux only; same uid and gid as clients."
            );
            return Ok(());
        }
        let value = args
            .next()
            .ok_or_else(|| invalid(format!("missing value for {option}")))?;
        match option.as_str() {
            "--name" => name = parse_name(&value)?,
            "--socket" => socket = Some(value.into()),
            "--root" => root = Some(value.into()),
            "--memory" => memory = size("memory", &value)?,
            "--nvme" => nvme = Some(size("nvme", &value)?),
            "--staging" => staging = size("staging", &value)?,
            "--write-budget" => write_budget = size("write-budget", &value)?,
            "--metrics" => metrics = Some(value),
            other => return Err(invalid(format!("unknown option {other}"))),
        }
    }
    if memory < 4096 || !memory.is_multiple_of(4096) {
        return Err(invalid(
            "--memory must be a multiple of 4096 and at least 4096",
        ));
    }
    if staging < 8 * 1024 * 1024 || !staging.is_multiple_of(4096) {
        return Err(invalid(
            "--staging must be a multiple of 4096 and at least 8 MiB",
        ));
    }
    let nvme = match (&root, nvme) {
        (Some(_), None) => {
            return Err(invalid("--nvme is required with --root"));
        }
        (Some(_), Some(value)) if value < 8192 => {
            return Err(invalid("--nvme must be at least 8192 with --root"));
        }
        (Some(_), Some(value)) => value,
        (None, None | Some(0)) => 0,
        (None, Some(_)) => return Err(invalid("--nvme must be 0 when --root is omitted")),
    };
    let socket =
        socket.unwrap_or_else(|| PathBuf::from(format!("/run/flare-crosscache/{name}.sock")));
    if socket.as_os_str().len() > 107 {
        return Err(invalid("--socket must be at most 107 bytes"));
    }
    let config = Config {
        name,
        root,
        socket,
        memory: usize::try_from(memory).map_err(|_| invalid("--memory too large"))?,
        nvme,
        staging: usize::try_from(staging).map_err(|_| invalid("--staging too large"))?,
        metrics,
        faults: Default::default(),
        write_budget,
        max_window_bytes: flare_crosscache::store::DEFAULT_MAX_WINDOW,
    };
    control::run(config, Arc::new(AtomicBool::new(false)))
}

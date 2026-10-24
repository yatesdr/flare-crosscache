# Flare cache manager

A Linux CPU-only cache service for Flare replicas sharing one host. It owns the
prefix tree, record admission, deduplication, LRU eviction, row compaction,
leases, DRAM retention, NVMe writes and recovery. It never accesses GPU memory.
The client contract is [PROTOCOL.md](PROTOCOL.md), protocol version 1.

## Build and run

Use Rust 1.87 or newer (validated with 1.94.1 on Linux). Serving is async on
tokio and speaks gRPC through tonic and prost over a Unix socket; it also
depends on `blake2` and `twox-hash`.

```sh
cargo build --release --locked
mkdir -p /run/flare-cache /nvme/flare-cache
target/release/selene \
  --socket /run/flare-cache/manager.sock \
  --root /nvme/flare-cache \
  --memory 8G --nvme 256G --staging 64M \
  --metrics 127.0.0.1:9108
```

Run with the same effective uid and gid as the replicas. A missing
socket directory is created with permissions 0770, an existing one keeps its
mode and ownership, and the socket itself is 0660. One process exclusively
locks each root, and startup refuses to replace a live socket. A stale socket
from a killed manager is removed once startup confirms nothing is listening. The
NVMe file system must support 4096-byte O_DIRECT reads/writes, rename and
file/directory sync. Do not use a network file system.

Options accept bytes or binary K/M/G/T suffixes. Defaults:

| Option | Default |
|---|---|
| `--name` | `default`; names the shared-memory instance and the default socket |
| `--socket` | `/run/selene/<name>.sock` |
| `--root` | none; records stay in DRAM until evicted |
| `--memory` | 1 GiB |
| `--nvme` | 0 without `--root`; required with `--root` |
| `--staging` | 256 MiB (minimum 8 MiB) |
| `--write-budget` | 0 (unlimited) |
| `--metrics` | disabled; when set, HTTP `GET /metrics` |

The arena and staging are preallocated files under `/dev/shm/selene/<name>`,
mapped shared with THP advice and split into 4096-aligned, reference-counted
size-class extents, so the shared-memory mount must hold at least
`--memory + --staging`. Size classes round allocations to powers of two; usable
record capacity can therefore be lower than the configured mapping size.
Budgets cover cache/storage extents, not total process RSS: trees, checksum
tables, bounded frame channels and each disk worker's small aligned scratch
buffers also consume memory. NVMe admission includes the 4096-byte manager
envelope and the complete padded Flare record. All namespaces share these
budgets. Records whose rounded extent exceeds the arena stream through staging
and also charge a fixed two-window staging hold; a record that fits the arena
waits for DRAM room, and streams as well when the arena cannot make room for
it.

A DRAM publication is acknowledged before NVMe durability. A manager crash can
lose recently acknowledged records whose ancestor chains have not reached NVMe.
Durable records survive replica/manager restarts. Disk-full writes retain
non-durable DRAM copies and retry at 100 ms intervals. Clients receive Budget
retries and a Room notification when capacity becomes available. DRAM eviction
requires durability; NVMe eviction removes both copies. SIGTERM/SIGKILL ends the
process; recovery removes unfinished files and orphaned descendants on restart.

The service runs a state thread that owns the cache, a tokio gRPC runtime, an
acceptor, one ordered writer per namespace, and an optional metrics thread.
Each connection has bounded queues, four open operations per direction and two
window credits per direction. Metrics include namespace/tier inventory and hits,
Put/Get outcomes, evictions, compactions, disk bytes/time/errors, connections,
leases, open operations, cancellations, discarded frames and arena allocations.

## Docker

```sh
docker build -t selene .
docker run --rm --name selene \
  --user "$(id -u):$(id -g)" --pid=host --network=host --ipc=host \
  -v /run/selene:/run/selene \
  -v /nvme/flare-cache:/var/lib/flare-cache \
  selene --root /var/lib/flare-cache --nvme 256G --memory 8G \
  --metrics 127.0.0.1:9108
```

Create `/run/selene` and the root directory with ownership matching the runtime
uid/gid. The arena and staging live in `/dev/shm/selene` on the host, so the
manager and every replica must share that mount: run all of them with
`--ipc=host` as above, or bind-mount the same directory at `/dev/shm/selene`
in each container. The host's `/dev/shm` must hold `--memory + --staging`.
Share the socket directory and a PID namespace with the replicas so the peer
pid matches `SO_PEERCRED`; the example uses the host PID namespace. No GPU
flags, CUDA libraries, privileges, or network cache transport are required.

## Tests

Run on Linux. All 64 L0 tests, 41 library tests and the protocol test execute by
default, including subprocess SIGKILL tests and exact-byte `/dev/shm` and
real-NVMe throughput tests. There are no ignored tests or GPU requirements. Set scratch to a disposable directory on a
real local NVMe file system; each test owns and removes its own subdirectory.

```sh
CM_NVME_SCRATCH=/nvme/cm-scratch cargo test --release --locked -- --nocapture
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
```

Run the tests on Linux with:

```sh
cargo test --release --locked -- --nocapture
```

The suite covers tree parity; eviction/compaction; accepted record shape and
replacement; deduplication/limits; malformed payloads; ordered chain durability;
manager crash/recovery; streaming/refill/Oversize and the admission charge;
disk-full/Budget/Room; restore windows under a full durable budget;
Cancel/EOF/replica SIGKILL and I/O lifetimes; paused Put windows and quarantine;
snapshots/deltas/Resync/namespaces; socket and shared-memory directory modes;
round-robin service and bounded cleanup; generation/op fencing; and throughput.
Fault gates exist only in the library's test configuration and are not exposed
by the executable.

The throughput group runs an in-process manager and one client across three
modes - DRAM through shared memory, staged streaming through shared memory, and
staged streaming on NVMe - with 128 64 KiB pages (about 8 MiB) per record and
six samples per mode, the first a warm-up. It compares the exact fetched bytes
and requires a DRAM median of at least 1 GB/s in both directions; the streamed
modes report their medians without a floor. Set `CM_NVME_SCRATCH` to a
disposable directory on a local NVMe file system for the NVMe mode. The test
needs room for the DRAM record in `/dev/shm`; test containers need
`--shm-size=4g`.

```sh
CM_NVME_SCRATCH=/nvme/cm-scratch cargo run --release --locked --example throughput
```

See [PERFORMANCE.md](PERFORMANCE.md) for before/after results, profiling and
measurement boundaries. A local data-path benchmark with one manager, one or
four clients and 64-560 MiB records measures 2.7-4.2 GB/s Put and 1.3-4.5 GB/s
Get through the shared-memory windows for DRAM records, and 0.6-0.8 GB/s Put
and 0.5-0.7 GB/s Get for streamed NVMe records; the manager stays at 8-31% CPU.
Page and row checksums cost 5.5 GB/s and memcpy 15-17 GB/s on the same host, so
a single client is limited by its window copies and the protocol roundtrip, not
by the manager. These are CPU transport measurements; the Flare client and GPU
integration validation live in the Flare repository.

Validated on a Linux NVMe host with Rust 1.94.1: 106 tests passed (41 library,
64 L0, 1 protocol), 0 failed, 0 ignored; rustfmt and clippy (`-D warnings`)
clean. The subprocess cases issue real SIGKILL at partial Data, before rename,
after rename and during the parent write, and kill a replica holding a parent
pin.

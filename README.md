# Flare cache manager

A Linux CPU-only cache service for Flare replicas sharing one host. It owns the
prefix tree, record admission, deduplication, LRU eviction, row compaction,
leases, DRAM retention, NVMe writes and recovery. It never accesses GPU memory.
The client contract is [PROTOCOL.md](PROTOCOL.md), protocol version 1.

## Build and run

Use Rust 1.87 or newer (validated with 1.94.1 on Linux). Dependencies are `libc`
and `xxhash-rust`; there is no async runtime.

```sh
cargo build --release --locked
mkdir -p /run/flare-cache /nvme/flare-cache
target/release/flare-cache-manager \
  --socket /run/flare-cache/manager.sock \
  --root /nvme/flare-cache \
  --memory 8G --nvme 256G --staging 64M \
  --metrics 127.0.0.1:9108
```

Run with the same effective uid and gid as the replicas. Parent socket-directory
permissions are set to 0770, socket permissions to 0660. One process exclusively
locks each root, and startup refuses to replace a live socket. A stale socket
from a killed manager is removed after acquiring the root lock. The NVMe file
system must support 4096-byte O_DIRECT reads/writes, rename and file/directory
sync. Do not use a network file system.

Options accept bytes or binary K/M/G/T suffixes. Defaults:

| Option | Default |
|---|---|
| `--socket` | `/run/flare-cache/manager.sock` |
| `--root` | `/var/lib/flare-cache` |
| `--memory` | 1 GiB |
| `--nvme` | 64 GiB |
| `--staging` | 64 MiB (minimum 8 MiB) |
| `--metrics` | disabled; when set, HTTP `GET /metrics` |

DRAM is an anonymous THP-advised mapping with 4096-aligned, reference-counted
size-class extents. Size classes round allocations to powers of two; usable
record capacity can therefore be lower than the configured mapping size.
Staging is a separate bounded mapping. Budgets cover cache/storage extents,
not total process RSS: trees, checksum tables, bounded frame channels and each
disk worker's small aligned scratch buffers also consume memory. NVMe admission
includes the 4096-byte manager envelope and complete padded Flare record.
All namespaces share these budgets. Only records larger than the retention
arena stream directly through staging; a fitting record waits for DRAM room.

A DRAM publication is acknowledged before NVMe durability. A manager crash can
lose recently acknowledged records whose ancestor chains have not reached NVMe.
Durable records survive replica/manager restarts. Disk-full writes retain
non-durable DRAM copies and retry at 100 ms intervals. Clients receive Budget
retries and a Room notification when capacity becomes available. DRAM eviction
requires durability; NVMe eviction removes both copies. SIGTERM/SIGKILL ends the
process; recovery removes unfinished files and orphaned descendants on restart.

The service uses a state thread, an acceptor, reader/writer threads per
connection, one ordered writer per namespace, and an optional metrics thread.
Each connection has bounded queues and four open operations per direction.
Metrics include namespace/tier inventory and hits, Put/Get outcomes, evictions,
compactions, disk bytes/time/errors, connections, leases, open operations,
cancellations, discarded frames and arena allocations.

## Docker

```sh
docker build -t flare-cache-manager .
docker run --rm --name flare-cache-manager \
  --user "$(id -u):$(id -g)" --pid=host --network=host \
  -v /run/flare-cache:/run/flare-cache \
  -v /nvme/flare-cache:/var/lib/flare-cache \
  flare-cache-manager --memory 8G --nvme 256G \
  --metrics 127.0.0.1:9108
```

Create both host directories with ownership matching the runtime uid/gid.
Share the socket volume and a PID namespace with the replicas so Hello's pid
matches SO_PEERCRED. The example uses the host PID namespace. No GPU flags,
CUDA libraries, privileges, or network cache transport are required.

## Tests

Run on Linux. All fourteen L0 groups execute by default, including subprocess
SIGKILL tests and exact-byte `/dev/shm`/real-NVMe throughput tests. There are no
ignored tests or GPU requirements. Set scratch to a disposable directory on a
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

The fourteen groups cover tree parity; eviction/compaction; accepted record
shape and replacement; deduplication/limits; malformed payloads; ordered
chain durability; manager crash/recovery; streaming/refill/Oversize;
disk-full/Budget/Room; Cancel/EOF/replica SIGKILL and I/O lifetimes;
snapshots/deltas/Resync/namespaces; round-robin service and bounded cleanup;
generation/op fencing; and throughput. Fault gates exist only in the library's
test configuration and are not exposed by the executable.

Throughput output reports exact Flare-record bytes, elapsed seconds and MiB/s.
The Put interval includes client payload construction/checksums, IPC, validation,
and waiting for chain durability. Get includes IPC and client byte collection;
exact-byte comparison follows timing. These are end-to-end L0 CPU measurements,
not raw NVMe rates or the L1 GPU/socket bandwidth gate. Run 2's Flare client and
L1–L3 GPU/integration validation are separate work.

Validated on a Linux NVMe host with Rust 1.94.1: 14 L0 groups passed, 0 failed, 0 ignored;
rustfmt and clippy (`-D warnings`) clean. Each throughput transfer contained
16,818,176 Flare-record bytes:

| Storage | Put MiB/s | Get MiB/s |
|---|---:|---:|
| `/dev/shm` | 333.50 | 1377.58 |
| NVMe (`CM_NVME_SCRATCH`) | 271.68 | 496.01 |

The subprocess cases issue real SIGKILL at partial Data, before rename, after
rename and during the parent write, and kill a replica holding a parent pin.

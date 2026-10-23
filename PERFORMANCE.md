# Performance audit

Measured on 2026-10-23, Rust 1.94.1, Linux, Threadripper PRO 3945WX. All results
below use one client and a separate manager process, three samples per mode,
8,960 pages of 65,536 bytes, and 4 MiB Data frames. Each record contains
587,284,480 bytes: 560 MiB of page payload plus header, checksum table and row.
Rates are decimal GB/s; the table reports medians, including the first sample.

| Path | Before Put | Before Get | After Put | After Get |
|---|---:|---:|---:|---:|
| DRAM publication, tmpfs backing | 2.455 | 3.334 | 4.337 | 4.110 |
| Streaming tmpfs, no DRAM retention | 1.996 | 2.266 | 2.052 | 3.232 |
| Streaming NVMe, no DRAM retention | 1.731 | 1.525 | 1.755 | 1.836 |

Before is manager commit `8a41100`, rebuilt with the final benchmark client and
compatibility accessors for that client's header/frame API. Its production
receive, send, checksum and disk paths were unchanged. Thus payload geometry,
verification and timing boundaries match the after measurement. The temporary
baseline source was removed after the comparison.

Put runs from Put request through Stored, including the manager's page/row
verification. For DRAM, median Stored latency was 135.413 ms; the separate
chain-durable timer was 406.649 ms from the same start. Streaming Stored follows
durability, so its Put timer includes disk work. Get includes direct socket
receives into the destination and per-page/row checksum verification. Every
sample also compares all bytes after timing. Payload generation, client input
checksums and destination allocation are prepared before timing, and their
elapsed time is printed separately. There is no discarded warm-up sample.

The original 16 MiB numbers (333.50/1377.58 MiB/s on tmpfs) combined client
payload generation and a durability wait into Put and used 4 KiB pages, hence
256 KiB windows. Get allocated/copied each frame repeatedly. They did not
measure the design's 4 MiB-frame socket basis. An initial 560 MiB run with that
client's copying measured 0.814/0.855 GB/s even with a DRAM-sized manager. The
paired table above removes those benchmark differences.

## Profile and changes

CPU-clock perf samples over the 560 MiB workload identified repeated memcpy,
receive-buffer zeroing and the SSE2 xxh3 path. Kernel page clearing and socket/
tmpfs copies remain significant after removing the redundant user-space copies.

The transport now writes frame headers and borrowed payloads with writev,
reads framing separately from payload, and recycles two bounded receive buffers
per connection. Get writes directly from retained extents or aligned disk
buffers. Incoming frames wake the state owner instead of waiting for its polling
timeout. A full disk queue retains its staged chunk instead of copying the
same frame again on each retry. Complete pages use one-shot xxh3; split pages
and rows retain incremental hashing. The checksum dependency is Flare's `twox-hash`, with runtime SIMD
selection, without requiring a native-CPU build or adding dependencies.

DRAM publication still precedes asynchronous parent-ordered disk writes.
O_DIRECT, file sync, rename and directory sync remain on the disk worker. The
streaming tmpfs Put remains below 3 GB/s. The benchmark's DRAM gate passes;
no claim is made that the durability-inclusive streaming Put reaches 3 GB/s.

## Size and validation

Production Rust decreased from 4,109 to 3,568 physical lines, counting comments
and blanks with the same rustfmt settings. This is a 13.2% reduction and remains
above the design's approximate 2,400-line estimate. The implementation removes
the duplicate record codec, client-only header encoder, unused manager lookup/
bucket index and pending-node state, duplicate window checksum storage, repeated
pin cleanup, special current-connection broadcast path, duplicate disk publish
paths and custom positional-I/O loops. Required protocol validation and crash,
lease, budget and durability behavior remain covered.

The fourteen L0 groups pass, with zero failures or ignored tests. Added checks
cover a fixed wire-header fixture, layout overflow, pages split across Data
frames, immediate rejection of invalid frame lengths, and live Get latency/
cancellation while a deliberately slow disk queue is full. Rustfmt and clippy
with `-D warnings` are clean. Benchmark source is shared by L0 group 14 and
`examples/throughput.rs`; the L0 group enforces median DRAM Put/Get >= 3 GB/s.

```sh
cargo test --release --locked -- --nocapture
cargo run --release --locked --example throughput
```

The test container shm size was raised to 4 GiB so three complete
560 MiB records fit. Streaming tests explicitly force no DRAM retention. Perf
profile recordings preceded the final separate-
process benchmark arrangement and are diagnostic, not the acceptance timings.

# Qualification

How `selene` and the Flare pooled-prefix client were validated, what each check
proves, and where the numbers come from. Raw evidence (test output, benchmark
logs, oracle JSON) is kept with the work that produced it; this document is the
reviewer's summary.

## Test ladder

Each rung uses the real component below it and adds one class of risk.

| Rung | Runs on | What it proves |
|---|---|---|
| Unit and L0 | Linux CPU, no GPU | The manager's contract in isolation: tree and catalog ordering, admission and refusal classification, window grants and credits, budget and Room, eviction and compaction, chain durability, crash recovery, directory modes, and a throughput smoke test. Run against exact-byte regions in shared memory and a real local NVMe file system. |
| L1 | One GPU, a real manager process, the real client | Exact bytes through the whole client-server path: cross-process DRAM round trips including partial tails and padded planes/rows, a durable chain longer than DRAM, streamed records larger than DRAM, interrupted transfers on a new generation, corruption rejection, a paused window owner, a multi-record chain with non-aligned boundaries, and a BUDGET refusal followed by Room. Six CUDA scenarios; they are ignored by default because they need the GPU and manager environment. |
| L2 | Two GPU replicas, one manager | The shared lifecycle end to end: publish on one replica, evict the GPU copy, restore on the other from DRAM, evict DRAM, restore from NVMe, kill the publisher mid-copy, restart a replica, kill and restart the manager, restore again. Facts are judged with the distribution oracle below; a miss is legitimate only when a manager-off replica produces the same answer. |
| L3 | GPU replicas across a topology/tier matrix | TP1, TP2 and TP4 at 8k, 32k, 100k and 262k-token contexts over the DRAM and NVMe tiers, plus write-through and offload legs. Each DRAM/NVMe row is compared with the GPU-native-reuse row at the same topology and context, so scaling and sharding do not hide numeric effects. |
| L4 | Two managers, CPU only | The remote tier's protocol between managers: peer catalog, forwarding, transfer and grant fencing, peer loss, and quota behavior, without GPUs. |

Counts at the time of writing: 108 default manager tests (library, L0 and
protocol-digest), 179 offload client tests plus 14 ignored GPU scenarios, and
the 130-case Flare binary suite.

## The correctness oracle

**Exact tokens cannot be used.** Flare clamps a requested temperature of 0 into
the package's declared sampling range (0.7-1.0 for the validated package) and
fixes top-k, so generation is sampled, not greedy. A fixed seed makes the
*distribution* reproducible, not the sampled token: a tiny numeric shift can
move the cumulative draw across a token boundary. Measured on one GPU, two cold
full prefills in separate processes differ by up to about 3 nats per token and
their sampled tokens diverge after 10-45 positions. That is process-to-process
numeric noise, not a cache defect.

**A correctness request** enables `logprobs`, `top_logprobs=20`, token-id text
and `ignore_eos`, and generates a fixed 48 tokens. Positions are compared in
stream order up to and including the first divergence, where the contexts are
still identical:

- the top-5 token sets are recorded (a near-tie reordering is noise, not a
  defect);
- for every token present in both top-20 lists with logprob at least -5 nats,
  the logprob difference is recorded.

A comparison reports the mean and maximum difference over those positions; the
mean is the gate.

**Compare the same shape on the same GPU.** Resuming a prefill after a prefix
is a different execution shape from a full cold prefill, so a restore is
compared with a *native* partial reuse on the same GPU at the same boundary, not
with a cold run. Cold/cold and native/native runs measure the noise floor; a
restore is judged against those, never against zero.

**Self-calibrating floor.** Each run takes two cold samples per seed on the same
GPU for every topology and context, and pools those cold/cold comparisons into
one floor for that topology and context: the pooled mean is the mean of all the
measured cold/cold means, so one noisy seed cannot set the bar for the whole
context. Only the GPU pass measures cold baselines, so every tier at that
topology and context uses the identical pooled floor, and the row records its
sample count.

The floor answers two different questions, neither of them a fixed tolerance:

- **Selene (pass/fail).** A DRAM or NVMe restore is judged against the
  GPU-native-reuse row at the *same topology and context*, so both sides have
  the same execution shape. It passes when its mean delta is at most
  max(1.5 x native mean, native mean + 0.15), with an absolute 10-nat guard on
  any single difference (corruption or a wrong record; malformed or non-finite
  top-20 data fails outright, as does a missing observation). The reference
  must be valid: a native row that ran cleanly, restored the whole prompt and
  compared at least the minimum number of positions. Without one the restore
  fails as "no valid native reference"; there is no fixed fallback, and the row
  is judged only against a native row from the same reference realisation (the
  prompt and cold answers are persisted and fingerprinted, so a resumed run
  reuses them rather than mixing processes).
- **Flare reuse (observation).** The GPU-native row is reported against the
  pooled cold/cold floor. How far Flare's own reuse sits from a cold prefill is
  a Flare numerics observation for the model server, not a selene failure.

A row's maximum delta, top-5 difference fraction and exact tokens are recorded
for diagnosis but are not gates: a per-row maximum over a few dozen positions
against a three-seed floor is an extreme-value statistic and does not
discriminate.

At 8k and 32k a row compares several hundred positions. At 100k and 262k the
sampled sequences diverge early and one seed adds only a few positions, so those
rows add seeds (up to a hard cap of 12) until they reach 200 compared positions;
the row records its seed schedule and compares count, and a row that hits the cap
reports the count it reached. The count is part of the report, because the mean
of 40 heavy-tailed deltas is not the same evidence as the mean of 700.

## Five-seed control

One GPU per run, one seed per run, the same fixed conversation (about 33.8k
tokens of prefix) and the same correctness requests. A: cold against cold in
separate processes. B: cold against native partial reuse. B2: two independent
native partial-reuse runs. C: native partial reuse against a selene host
restore. Values are max delta (nats) / mean delta / fraction of positions whose
top-5 sets differ.

| Seed | A cold/cold | B cold/native | B2 native/native | C native/restore |
|---|---|---|---|---|
| 1234 | 2.955 / 0.186 / 0.50 | 2.858 / 0.151 / 0.56 | 1.815 / 0.163 / 0.54 | 0.954 / 0.103 / 0.67 |
| 12345 | 1.534 / 0.137 / 0.69 | 1.952 / 0.151 / 0.56 | 3.183 / 0.442 / 0.55 | 2.285 / 0.122 / 0.69 |
| 7 | 1.892 / 0.253 / 0.55 | 1.740 / 0.206 / 0.64 | 1.952 / 0.192 / 0.48 | 2.042 / 0.226 / 0.69 |
| 99 | 1.809 / 0.164 / 0.52 | 1.930 / 0.183 / 0.53 | 1.751 / 0.141 / 0.46 | 0.731 / 0.102 / 0.51 |
| 2026 | 1.689 / 0.197 / 0.54 | 1.116 / 0.102 / 0.59 | 2.053 / 0.222 / 0.52 | 2.229 / 0.160 / 0.46 |

The largest A/B2 value per statistic is the envelope: max 3.183, mean 0.442,
top-5 fraction 0.69. Every C row lies below it, with the largest restore max at
0.72x the envelope and the largest restore mean at 0.51x. The restore is
therefore indistinguishable from native reuse at this scale; the noise is the
sampling process, not the pool.

## Local data-path benchmarks

Measured on one Linux host with NVIDIA SM121 GPUs and local NVMe, shared with
GPU work (load average around 10), CPU only for the data path. One external
manager, local Unix-socket clients moving records through the manager's
shared-memory windows, content generated outside the timed region, medians over
four samples with a warm-up.

| Path | Record | Clients | Put GB/s | Get GB/s | Client CPU | Manager CPU |
|---|---|---|---|---|---|---|
| DRAM | 64 MiB | 1 | 2.69 | 1.26 | 83% | 21% |
| DRAM | 64 MiB | 4 | 3.63 | 4.54 | 230% | 31% |
| DRAM | 560 MiB | 1 | 3.32 | 1.46 | 90% | 11% |
| DRAM | 560 MiB | 4 | 4.22 | 4.55 | 280% | 20% |
| NVMe streaming | 64 MiB | 1 | 0.59 | 0.48 | 29% | 8% |
| NVMe streaming | 560 MiB | 1 | 0.77 | 0.65 | 39% | 9% |

Baselines on the same host: page and row checksums 5.5 GB/s, single-thread
memcpy 15.4-17.1 GB/s, raw NVMe direct I/O about 1.5 GB/s write and 1.6 GB/s
read. The manager stays at 8-31% CPU in every case, so it is not the limiter.

The single-client DRAM Get was later shown to depend on the client's buffer
lifetime: the first harness allocated a fresh destination for every fetch and
reached 1.4-1.5 GB/s; reusing a pre-touched destination raised it to 2.54-2.56
GB/s, and also reusing the read buffer reached 3.05-3.09 GB/s. Production
clients copy into pooled, reused slabs, so the lower figure was a harness
effect, not a serving cost.

## L3 results

To be filled when the L3 matrix run completes. Each row reports the topology,
context length and tier, the cold/cold floor measured in the same run, and the
comparison's mean and max deltas and the mean gate above.

| Topology | Context | Tier | Compared | Floor mean/max | Row mean/max | Top-5 fraction | Result |
|---|---|---|---|---|---|---|

## Limitations

- Distribution equivalence is the correctness criterion. Exact token sequences
  and top-5 sets are not reproducible across processes at this sampling
  configuration and are reported, not gated.
- The L2/L3 floor is measured per run on the same hardware; a result from one
  host does not transfer to another uncalibrated.
- The benchmarks measure the CPU data path and shared-memory windows. They do
  not include GPU transfer, scheduling or model execution time.

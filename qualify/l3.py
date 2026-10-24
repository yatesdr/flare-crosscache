#!/usr/bin/env python3
"""L3: topology matrix, per-tier timing, loaded throughput and the off-path A/B.

Runs model packages over six topologies (1/2/4 replicas of TP1, 1/2 of TP2, 1xTP4) under three
cache configurations: GPU only (no manager), a DRAM-only manager, and a manager with NVMe
whose arena is deliberately small and churned by filler prompts.

The oracle is a true cold recompute per topology, tier and request seed: the identical message
list, sent to a replica with no manager and with the GPU prefix cache busted, once per seed, twice
to record that topology/tier's cold-vs-cold noise floor. The per-seed cold/cold comparisons are
pooled into one floor per topology and context (the mean of all their means). Two different
questions are asked of it, with two different gates:

- SELENE, pass/fail: each DRAM/NVMe restore is compared with the GPU-native-reuse row at the same
  topology and context, so both sides have the same execution shape. It passes when its mean delta
  is within max(1.5 x native mean, native mean + 0.15), with the absolute 10-nat guard and a
  well-formed-data requirement.
- FLARE REUSE, informational: the GPU-native row is reported against the pooled cold/cold floor.
  How far Flare's own reuse sits from a cold prefill is a Flare numerics observation, not a
  selene failure.

Each attempt keeps the original single
timed request (one per attempt, no logprobs, so the latency statistics are untouched) and then
runs the three seeded correctness requests separately, outside the latency window. Those carry
generation logprobs on both sides. The maximum, top-5 fraction and exact tokens are
recorded but not gated. Sampling is not greedy: temperature 0 is clamped into the model package's
declared range (the default when none is declared), so a fixed seed fixes the distribution, and a
sampled-token divergence under matching distributions is benign. Before every timed restore the
consumer replica's GPU copy is evicted, and the NVMe arena is re-churned for every repetition; the
manager's per-tier hit and fetch counters are snapshotted around each timed request, so a DRAM or
NVMe row passes only if that request read from the expected tier. l3.json is rewritten after every
row so a crash preserves the measured floors; a mode's manager records are deleted once its
rows are recorded, and a row is refused when the workdir has less than --min-free GiB free.

Write-through cost is measured on a sustained workload: several concurrent streams, each with a
distinct fresh long prefix so every request publishes, over alternating manager-on/off trials,
gated at a small relative cost. The off-path A/B compares the current Flare binary with no
manager flag against the pre-change binary on 1xTP1 and 1xTP4, same configuration, throughput
and TTFT within a tolerance.

GPU runs are expensive and are scheduled separately; --dry-run validates every manager command
line, replica port/GPU assignment, instance name and work directory and prints the plan without
starting anything. Cluster helpers come from Flare's qualify package.

Results go to l3.json and a markdown table (l3.md).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import metrics
import oracle

TIER_SENTENCES = {"8k": 500, "32k": 2000, "100k": 6250, "262k": 14000}
DEFAULT_TIERS = ["8k", "32k", "100k", "262k"]
MODES = ("gpu", "dram", "nvme")
ANSWER_TOKENS = 48
# At long context the sampled sequences diverge early, so a single seed contributes few
# compared positions. Long tiers add seeds (up to --long-seeds) until a row has
# --long-positions comparisons, so its mean is not a few dozen heavy-tailed deltas.
LONG_TIERS = ("100k", "262k")
MAX_LONG_SEEDS = 12
EXTRA_SEEDS = [12348, 12349, 12350, 12351, 12352, 12353, 12354, 12355, 12356, 12357,
               12358, 12359]
LOAD_TOKENS = 256


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manager", type=Path, required=True)
    parser.add_argument("--flare", type=Path, required=True)
    parser.add_argument("--flare-root", type=Path, required=True)
    parser.add_argument("--tp1-package", type=Path, action="append", default=[],
                        help="TP1 package; repeat four times for the 4xTP1 row")
    parser.add_argument("--tp2-package", type=Path, action="append", default=[],
                        help="TP2 package; repeat twice for the 2xTP2 row")
    parser.add_argument("--tp4-package", type=Path, action="append", default=[],
                        help="TP4 package")
    parser.add_argument("--main-flare", type=Path, help="pre-change flare binary for the A/B")
    parser.add_argument("--flare-arg", action="append", default=[],
                        help="extra argument for every replica's `flare run`; repeatable")
    parser.add_argument("--gpus", default="0,1,2,3")
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--memory", default="16G",
                        help="manager DRAM for the +DRAM tier; at least twice the largest "
                             "target record")
    parser.add_argument("--nvme-memory", default="16M",
                        help="manager DRAM for the +NVMe tier; small enough that one record "
                             "plus the fillers always evict the measured prompt")
    parser.add_argument("--nvme-fill", type=int, default=32,
                        help="filler prompts that churn the +NVMe arena per repetition")
    parser.add_argument("--nvme", default="64G")
    parser.add_argument("--staging", default="512M")
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--seeds", default="12345,12346,12347",
                        help="fixed request seeds per fact; each answer is compared with its "
                             "same-seed cold recompute")
    parser.add_argument("--long-seeds", type=int, default=12,
                        help="seed cap in the long tiers; seeds are added until a row has "
                             "--long-positions compared positions")
    parser.add_argument("--long-positions", type=int, default=200,
                        help="compared positions a long-tier row should reach")
    parser.add_argument("--oracle-envelope", default=None,
                        help="calibrated MEAN,MAX,TOP5[,ABSOLUTE] nats; without it the strict "
                             "per-position rule applies")
    parser.add_argument("--tiers", default=",".join(DEFAULT_TIERS))
    parser.add_argument("--load-seconds", type=float, default=60)
    parser.add_argument("--load-concurrency", type=int, default=8,
                        help="concurrent fresh-prefix streams during a load trial")
    parser.add_argument("--load-tokens", type=int, default=LOAD_TOKENS,
                        help="decoded tokens per load request")
    parser.add_argument("--write-through-trials", type=int, default=5,
                        help="alternating manager-on/off load trials")
    parser.add_argument("--write-through-tolerance", type=float, default=0.02,
                        help="relative throughput/TTFT cost allowed with the manager on")
    parser.add_argument("--offpath-tolerance", type=float, default=0.10,
                        help="relative throughput/TTFT difference allowed in the A/B")
    parser.add_argument("--skip-load", action="store_true")
    parser.add_argument("--only-write-through", action="store_true",
                        help="run only the sustained write-through trials (no topology rows)")
    parser.add_argument("--resume", action="store_true",
                        help="keep passing rows already recorded in the workdir's l3.json "
                             "and run the rest, but only when their fingerprint matches")
    parser.add_argument("--manager-commit", default=None,
                        help="manager build source revision, recorded in row fingerprints")
    parser.add_argument("--flare-commit", default=None,
                        help="Flare build source revision, recorded in row fingerprints")
    parser.add_argument("--skip-offpath", action="store_true")
    parser.add_argument("--timeout", type=float, default=1200)
    parser.add_argument("--startup-timeout", type=float, default=900)
    parser.add_argument("--label", default=None,
                        help="label recorded in the plan and results (e.g. pre-fix)")
    parser.add_argument("--min-free", type=int, default=120,
                        help="refuse to start a row when the workdir's filesystem has less "
                             "than this many GiB free")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    args.gpus = [value.strip() for value in args.gpus.split(",")]
    args.tiers = [value.strip() for value in args.tiers.split(",") if value.strip()]
    args.seeds = [int(value) for value in args.seeds.split(",") if value.strip()]
    if not args.seeds or any(seed < 0 for seed in args.seeds):
        parser.error("--seeds needs nonnegative integers")
    if args.oracle_envelope is None:
        args.envelope = oracle.envelope(oracle.FALLBACK_MEAN, oracle.FALLBACK_MAX, 1.0,
                                        oracle.ABSOLUTE_MAX)
    else:
        values = [float(value) for value in args.oracle_envelope.split(",") if value.strip()]
        if len(values) < 3:
            parser.error("--oracle-envelope needs MEAN,MAX,TOP5[,ABSOLUTE]")
        args.envelope = oracle.envelope(values[0], values[1], values[2],
                                        values[3] if len(values) > 3 else None)
    # Comparisons record statistics against the absolute guard only; each row is then gated
    # against its own topology/tier cold-vs-cold floor, with the fixed values as fallback.
    args.absolute_max = args.envelope["absolute_max"] or oracle.ABSOLUTE_MAX
    args.comparison_limits = oracle.envelope(absolute_max=args.absolute_max)
    args.fallback_mean = args.envelope["mean_delta"]
    args.fallback_max = args.envelope["max_delta"]
    if len(args.tp1_package) < 4:
        parser.error("pass four --tp1-package values (1xTP1, 2xTP1, 4xTP1)")
    if len(args.tp2_package) < 2:
        parser.error("pass two --tp2-package values (1xTP2, 2xTP2)")
    if len(args.tp4_package) < 1:
        parser.error("pass one --tp4-package value (1xTP4)")
    if len(args.gpus) < 4:
        parser.error("the matrix needs four GPUs")
    if args.reps < 1:
        parser.error("--reps must be at least 1")
    if args.load_concurrency < 1:
        parser.error("--load-concurrency must be at least 1")
    if args.load_tokens < 16:
        parser.error("--load-tokens must decode at least 16 tokens")
    if args.write_through_trials < 1:
        parser.error("--write-through-trials must be at least 1")
    if not 0 <= args.write_through_tolerance < 1:
        parser.error("--write-through-tolerance must be in [0, 1)")
    if args.offpath_tolerance < 0:
        parser.error("--offpath-tolerance must not be negative")
    for path in [args.manager, args.flare, *args.tp1_package, *args.tp2_package,
                 *args.tp4_package]:
        if not path.exists():
            parser.error(f"missing path: {path}")
    if args.main_flare is not None and not args.main_flare.exists():
        parser.error(f"missing path: {args.main_flare}")
    for tier in args.tiers:
        if tier not in TIER_SENTENCES:
            parser.error(f"unknown tier {tier}")
    if not (args.flare_root / "qualify" / "host_cache_isolation.py").exists():
        parser.error(f"{args.flare_root} does not contain qualify/host_cache_isolation.py")
    args.workdir.mkdir(parents=True, exist_ok=True)
    return args


def topologies(args) -> list[dict]:
    rows = [
        {"name": "1xTP1", "packages": args.tp1_package[:1], "per_replica": 1},
        {"name": "2xTP1", "packages": args.tp1_package[:2], "per_replica": 1},
        {"name": "4xTP1", "packages": args.tp1_package[:4], "per_replica": 1},
        {"name": "1xTP2", "packages": args.tp2_package[:1], "per_replica": 2},
        {"name": "2xTP2", "packages": args.tp2_package[:2], "per_replica": 2},
        {"name": "1xTP4", "packages": args.tp4_package[:1], "per_replica": 4},
    ]
    for row in rows:
        width = len(row["packages"]) * row["per_replica"]
        row["gpus"] = args.gpus[:width]
        row["replicas"] = [
            {
                "package": package,
                "gpus": row["gpus"][index * row["per_replica"]:(index + 1) * row["per_replica"]],
                "port": 8100 + index,
            }
            for index, package in enumerate(row["packages"])
        ]
    return rows


def load_helpers(root: Path):
    sys.path.insert(0, str(root))
    from qualify import host_cache, host_cache_isolation  # noqa: PLC0415

    return host_cache, host_cache_isolation


def seeded_body(host_cache, model, messages, tokens, seed) -> dict:
    """A completion request for these messages pinned to one request seed."""
    request = host_cache.body(model, messages, tokens)
    request["seed"] = seed
    return request


def manager_command(args, name, mode, socket, root, metrics_port) -> list[str]:
    command = [
        str(args.manager), "--name", name, "--socket", str(socket),
        "--memory", args.memory if mode == "dram" else args.nvme_memory,
        "--staging", args.staging, "--metrics", f"127.0.0.1:{metrics_port}",
    ]
    if mode == "nvme":
        command += ["--root", str(root), "--nvme", args.nvme]
    return command


class Manager:
    def __init__(self, args, name, mode, metrics_port, command, socket, root):
        self.args = args
        self.name = name
        self.mode = mode
        self.base = args.workdir / name
        self.base.mkdir(parents=True, exist_ok=True)
        self.socket = socket
        self.root = root
        self.log = (self.base / "manager.log").open("a")
        self.metrics_port = metrics_port
        self.command = command
        self.process = None

    def start(self):
        import subprocess  # noqa: PLC0415
        self.process = subprocess.Popen(self.command, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 120
        import socket  # noqa: PLC0415
        while True:
            assert self.process.poll() is None, "manager exited"
            try:
                with socket.socket(socket.AF_UNIX) as peer:
                    peer.connect(str(self.socket))
                return
            except OSError:
                assert time.monotonic() < deadline, "manager socket did not appear"
                time.sleep(0.1)

    def metrics(self) -> dict[str, float]:
        import urllib.request  # noqa: PLC0415
        try:
            text = urllib.request.urlopen(
                f"http://127.0.0.1:{self.metrics_port}/metrics", timeout=2
            ).read().decode()
        except OSError:
            return {}
        values: dict[str, float] = {}
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            name, _, value = line.partition(" ")
            try:
                values[name] = float(value)
            except ValueError:
                continue
        return values

    def tier_hits(self) -> dict[str, float]:
        hits: dict[str, float] = {}
        for name, value in self.metrics().items():
            if not name.startswith("cm_hits_total{"):
                continue
            for tier in ("memory", "disk"):
                if f'tier="{tier}"' in name:
                    hits[tier] = hits.get(tier, 0.0) + value
        return hits

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            self.process.wait(timeout=120)
        self.process = None
        if not self.log.closed:
            self.log.close()


class Replica:
    def __init__(self, host_cache, args, flare, package, gpu, port, label, extra):
        self.host_cache = host_cache
        self.args = args
        self.flare = flare
        self.package = package
        self.gpu = gpu
        self.port = port
        self.label = label
        self.log_path = args.workdir / f"{label}.log"
        self.extra = extra
        self.server = None

    def start(self):
        self.server = self.host_cache.Server(
            self.flare, self.package, ",".join(self.gpu) if isinstance(self.gpu, list) else self.gpu,
            self.port, self.log_path, self.extra,
        )
        self.server.wait_ready(self.args.startup_timeout)
        return self.server

    def stop(self):
        if self.server is not None:
            self.server.stop()
            self.server = None


def ttft_ms(result) -> float | None:
    performance = result.get("performance") or {}
    ingress = performance.get("ingress_ns")
    if result.get("core_ns") and isinstance(ingress, int):
        return (result["core_ns"][0] - ingress) / 1e6
    return None


def p99(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, round(0.99 * (len(ordered) - 1))))]


def seed_schedule(args, tier: str) -> list[int]:
    """The seeds a row may use: the requested ones, plus extras in the long tiers.

    A long-tier row runs its requested seeds always, then one extra seed at a time until it
    has reached --long-positions compared positions (checked in time_tier) or the cap. The
    cap is hard: a long-tier schedule never exceeds MAX_LONG_SEEDS, even when more seeds
    were requested.
    """
    seeds = list(args.seeds)
    if tier not in LONG_TIERS:
        return seeds
    cap = min(max(args.long_seeds, 1), MAX_LONG_SEEDS)
    if len(seeds) >= cap:
        return seeds[:cap]
    for seed in EXTRA_SEEDS:
        if len(seeds) >= cap:
            break
        if seed not in seeds:
            seeds.append(seed)
    return seeds


def cold_baseline(host_cache, args, replica, run, topology, tier):
    """True cold recomputes per request seed, with no manager and the GPU copy busted.

    A second sample per seed gives the cold-vs-cold noise floor under the same envelope; it
    is recorded, not used to pass or fail this run.
    """
    nonce = f"l3-{topology}-{tier}"
    model = replica.server.model_id()
    messages, _, _ = host_cache.conversation(run, model, nonce, f"{nonce}-cold",
                                             TIER_SENTENCES[tier])
    answers = {}
    noise = {}
    for seed in seed_schedule(args, tier):
        samples = []
        # The requested seeds get two samples (the cold/cold floor); extra long-tier seeds
        # only contribute their reference answer, so they need one.
        for attempt in range(2 if seed in args.seeds else 1):
            host_cache.evict_everything(run, model, nonce,
                                        f"{nonce}-cold-evict-{seed}-{attempt}", host=False)
            result = oracle.stream(
                replica.server.url,
                oracle.correctness_body(
                    seeded_body(host_cache, model, messages, ANSWER_TOKENS, seed)),
                args.timeout)
            if result["errors"] or not result["tokens"]:
                raise AssertionError(
                    f"{nonce} seed {seed} sample {attempt}: cold baseline failed: "
                    f"{result['errors'][:1]}")
            samples.append((tuple(result["tokens"]), result["logprobs"]))
        answers[seed] = {"tokens": samples[0][0], "logprobs": samples[0][1]}
        if len(samples) > 1:
            noise[seed] = oracle.agrees(samples[0][0], samples[0][1],
                                        samples[1][0], samples[1][1],
                                        limits=args.comparison_limits)
    return messages, answers, noise


def pooled_floor(noise: dict):
    """The cold/cold floor for one topology and context, pooled over every seed sample.

    Only the GPU pass of a topology measures cold baselines, so the DRAM and NVMe rows at
    the same topology and context are judged against the identical floor; there is nothing
    extra to pool across modes. Pooling the seed samples means the floor mean is the mean
    of all measured cold/cold means, so one unusually noisy seed cannot set the bar for the
    whole context. The largest per-seed maximum is kept for the record.
    """
    means = [evidence["mean_delta"] for evidence in noise.values()
             if isinstance(evidence, dict) and evidence.get("mean_delta") is not None]
    maxes = [evidence["max_delta"] for evidence in noise.values()
             if isinstance(evidence, dict) and evidence.get("max_delta") is not None]
    if not means:
        return None
    return {"mean_delta": statistics.mean(means),
            "max_delta": max(maxes) if maxes else None,
            "samples": len(means),
            "seed_max_mean": max(means)}


def native_row(facts: list[dict], topology: str, tier: str):
    """The GPU-native-reuse row for a topology and context, the like-for-like reference."""
    for row in facts:
        if (row.get("topology") == topology and row.get("tier") == tier
                and row.get("mode") == "gpu"):
            return row
    return None


def valid_native(native, minimum_positions: int):
    """A native row that can serve as a reference, or None.

    It must have run cleanly and restored the whole prompt, carry distribution statistics
    over at least the minimum number of compared positions, and have no mismatch. A row that
    failed, or that compared too few positions, cannot make a restore look good or bad.
    """
    if native is None or native.get("pass") is not True:
        return None
    if native.get("errors"):
        return None
    if native.get("correctness_mismatches"):
        return None
    if native.get("mean_delta") is None or native.get("max_delta") is None:
        return None
    if (native.get("compared_positions") or 0) < minimum_positions:
        return None
    reused, prompt = native.get("reused_p50"), native.get("prompt_p50")
    if reused is None or prompt is None or reused < prompt:
        return None
    return native


def failed_row(topology: str, mode: str, tier: str, reason: str, **fields) -> dict:
    """A row that never measured, with every field the report and recheck render."""
    row = {
        "topology": topology, "mode": mode, "tier": tier, "seeds": [], "reps": 0,
        "errors": [reason], "pass": False, "reason": reason,
        "compared_positions": 0, "max_delta": None, "mean_delta": None,
        "statistics": {}, "manager_counters": {}, "manager_records": {},
        "filler_bytes": 0.0, "top5_fraction": None, "correctness_mismatches": 0,
        "answer_tokens": None, "fetched": 0, "fetched_per_request": None,
        "memory_hits": 0.0, "disk_hits": 0.0, "ttft_p50_ms": None, "ttft_p99_ms": None,
        "reused_p50": None, "prompt_p50": None,
    }
    row.update(fields)
    return row


def apply_gates(row: dict, mode: str, cold_floor, native, minimum_positions: int,
                absolute_max: float) -> dict:
    """Attach the selene gate (DRAM/NVMe) or the Flare-reuse observation (GPU).

    A DRAM or NVMe restore is judged against a valid GPU-native-reuse row at the same
    topology and context, so both sides have the same execution shape. Without a valid
    native row the restore cannot be judged and fails: there is no fixed fallback. The GPU
    row has no selene gate: it is reported against the pooled cold/cold floor, which tells
    us about Flare's own reuse numerics, not about selene.
    """
    updated = dict(row)
    statistics_ = dict(updated.get("statistics") or {})
    errors = [error for error in updated.get("errors", [])
              if not str(error).startswith(("mean_delta", "max_delta",
                                            "no distribution"))]
    mean, maximum = updated.get("mean_delta"), updated.get("max_delta")
    statistics_["cold_floor"] = cold_floor
    if mode == "gpu":
        _, flare = oracle.gate(mean, maximum,
                               cold_floor["mean_delta"] if cold_floor else None,
                               cold_floor["max_delta"] if cold_floor else None,
                               absolute_max=absolute_max)
        statistics_["flare_reuse"] = flare
        statistics_["gate"] = None
        if mean is None or maximum is None:
            errors.append("no distribution statistics")
    else:
        reference_row = valid_native(native, minimum_positions)
        if reference_row is None:
            statistics_["native_reference"] = None
            statistics_["gate"] = {"passed": False, "failures":
                                   ["no valid native reference"],
                                   "mean_limit": None, "absolute_max": absolute_max}
            errors.append("no valid native reference")
        else:
            reference = {"mean_delta": reference_row["mean_delta"],
                         "max_delta": reference_row.get("max_delta")}
            statistics_["native_reference"] = reference
            _, gate = oracle.gate(mean, maximum, reference["mean_delta"],
                                  reference["max_delta"], absolute_max=absolute_max)
            statistics_["gate"] = gate
            errors.extend(gate["failures"])
    updated["statistics"] = statistics_
    updated["errors"] = errors
    updated["pass"] = not errors
    updated["reason"] = ("cache hit on every request" if updated["pass"]
                         else f"{len(errors)} error(s): {errors[0]}" if errors
                         else "not judged")
    return updated


def parse_size(text: str) -> int:
    """A byte size with a binary K, M, G or T suffix, as the manager accepts."""
    suffix = text[-1:].lower()
    multiplier = {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3, "t": 1024 ** 4}.get(suffix)
    if multiplier is None:
        return int(text)
    return int(text[:-1]) * multiplier


def prompt_digest(messages) -> str:
    """The identity of the conversation a cold reference was measured on."""
    return hashlib.sha256(json.dumps(messages, sort_keys=True).encode()).hexdigest()


def reference_record(messages, answers) -> dict:
    """A cold reference in a serialisable form, so a resumed run reuses the same one."""
    return {"prompt_sha256": prompt_digest(messages),
            "messages": messages,
            "answers": {str(seed): {"tokens": list(value["tokens"]),
                                    "logprobs": value["logprobs"]}
                        for seed, value in answers.items()}}


def reference_digest(reference: dict) -> str:
    """The identity of the whole reference realisation (prompt and cold answers)."""
    return hashlib.sha256(json.dumps(reference, sort_keys=True).encode()).hexdigest()


def load_reference(reference: dict):
    answers = {int(seed): {"tokens": value["tokens"], "logprobs": value["logprobs"]}
               for seed, value in reference["answers"].items()}
    return reference["messages"], answers


def file_digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1 << 20), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def fingerprint_settings(args) -> dict:
    """The run settings a resumed row must share, in their significant order."""
    return {
        "flare_args": list(args.flare_arg),
        "memory": args.memory,
        "nvme_memory": args.nvme_memory,
        "nvme_fill": args.nvme_fill,
        "staging": args.staging,
        "nvme": args.nvme,
        "gpus": list(args.gpus),
        "tiers": list(args.tiers),
        "seeds": list(args.seeds),
        "long_seed_cap": args.long_seeds,
        "long_positions": args.long_positions,
        "long_seed_schedule": {tier: seed_schedule(args, tier) for tier in LONG_TIERS},
        "reps": args.reps,
        "tp1_packages": [str(package) for package in args.tp1_package],
        "tp2_packages": [str(package) for package in args.tp2_package],
        "tp4_packages": [str(package) for package in args.tp4_package],
        "envelope": args.envelope,
    }


def run_fingerprint(args) -> dict:
    """Everything a resumed row must share with the current run to be reusable."""
    package_hashes = {}
    for package in [*args.tp1_package, *args.tp2_package, *args.tp4_package]:
        package_hashes.setdefault(str(package), file_digest(package))
    return {
        "manager": {"path": str(args.manager), "sha256": file_digest(args.manager),
                    "commit": args.manager_commit},
        "flare": {"path": str(args.flare), "sha256": file_digest(args.flare),
                  "commit": args.flare_commit},
        "main_flare": (None if args.main_flare is None else
                       {"path": str(args.main_flare),
                        "sha256": file_digest(args.main_flare), "commit": "26646d85"}),
        "package_hashes": package_hashes,
        **fingerprint_settings(args),
    }


def wait_published(host_cache, run, label, timeout, before):
    """Wait until the publish request's store has been accepted.

    The publication is asynchronous: without this wait the first timed request can race the
    store and miss even though later attempts restore from the same prefix. `before` is the
    stored counter captured before the publish was issued: a store that completes before
    this function starts still counts.
    """
    host_cache.wait_quiescent(run, f"{label}-publish-quiet", timeout)
    deadline = time.monotonic() + timeout
    while (run.metrics(f"{label}-publish-check")
           .get("flare_host_cache_stored_total", 0) <= before):
        if time.monotonic() >= deadline:
            raise AssertionError(f"{label}: the publish was not stored")
        time.sleep(0.05)


def time_tier(host_cache, isolation, args, replica, run, model, tier, mode, manager, topology,
              baselines, facts, cold_noise, fingerprint, previous=None):
    nonce = f"l3-{topology}-{tier}"
    label = f"{nonce}-{mode}"
    # The GPU pass measures the cold baselines for this topology and context, so the pooled
    # floor and the native-reuse reference are available to every mode here, including on
    # a resumed row.
    floor = pooled_floor(cold_noise.get(f"{topology}/{tier}", {}))
    seeds = seed_schedule(args, tier)
    native = native_row(facts, topology, tier)
    if args.resume and previous is not None and previous.get("pass") is True:
        if previous.get("fingerprint") == fingerprint:
            resumed = apply_gates(previous, mode, floor, native, args.long_positions,
                                  args.absolute_max)
            if resumed["pass"]:
                facts.append(resumed)
                print(f"{topology:8s} {mode:4s} {tier:5s} PASS (resumed)")
                return resumed
            print(f"{topology:8s} {mode:4s} {tier:5s} rerun: the current gate turns the "
                  f"resumed row to FAIL ({resumed['errors'][:1]})")
        else:
            changed = sorted(key for key in set(previous.get("fingerprint") or {})
                             | set(fingerprint)
                             if (previous.get("fingerprint") or {}).get(key)
                             != fingerprint.get(key))
            print(f"{topology:8s} {mode:4s} {tier:5s} rerun: fingerprint changed "
                  f"({', '.join(changed) or 'missing'})")
    # Manager counters around the whole row: Put/Get outcomes (STORED, BUDGET, QUEUE,
    # OVERSIZE, INVALID) are the evidence for a host-cache miss.
    manager_start = manager.metrics() if manager is not None else {}
    messages, expected = baselines[(topology, tier)]
    if mode != "gpu":
        # Publish the identical message list through this mode's replica so the tier under
        # test holds it. The prefix is seed independent; decode with the first measured seed.
        # The stored counter is captured before the request: the store can complete before
        # wait_published starts, and a later snapshot would wait for a second increment.
        stored_before = run.metrics(f"{label}-publish-before").get(
            "flare_host_cache_stored_total", 0)
        try:
            publish = oracle.stream(
                replica.server.url,
                oracle.correctness_body(
                    seeded_body(host_cache, model, messages, ANSWER_TOKENS, args.seeds[0])),
                args.timeout)
            cold = expected[args.seeds[0]]
            matched, evidence = oracle.agrees(cold["tokens"], cold["logprobs"],
                                              publish["tokens"], publish["logprobs"],
                                              limits=args.comparison_limits)
            if publish["errors"] or not matched:
                reason = (f"publish distribution differs from the cold baseline: "
                          f"{evidence['failures'][:1] or evidence}"
                          if not publish["errors"]
                          else f"publish failed: {publish['errors'][:1]}")
                row = failed_row(topology, mode, tier, reason)
                facts.append(row)
                print(f"{topology:8s} {mode:4s} {tier:5s} FAIL ({row['reason']})")
                return row
            wait_published(host_cache, run, label, args.timeout, stored_before)
        except Exception as error:
            # One stuck publication, including a transfer-drain TimeoutError, fails this
            # row, not the whole matrix: the remaining tiers still produce evidence.
            reason = f"publish failed: {type(error).__name__}: {error}"
            row = failed_row(topology, mode, tier, reason)
            facts.append(row)
            print(f"{topology:8s} {mode:4s} {tier:5s} FAIL ({reason})")
            return row
    observations = []
    errors = []
    compared_positions = 0
    max_delta = None
    weighted_mean = 0.0
    mean_deltas = []
    top5_mismatches = 0
    correctness_mismatches = 0
    filler_bytes = 0.0
    for attempt in range(args.reps):
        if mode != "gpu":
            if mode == "nvme":
                # Push the target out of the manager's small memory arena deliberately, but
                # bound the filler volume by two arenas and by half the disk quota, checked
                # before each publication. Fillers persist across attempts and tiers, so a
                # tier-sized fixed count would fill NVMe, evict the target itself, and leave
                # no room for the next tier's publication (which then is never stored).
                quota = parse_size(args.nvme)
                budget = min(2 * parse_size(args.memory), quota // 2)
                written_before = manager.metrics().get("cm_nvme_bytes_total", 0.0)
                written = 0.0
                for index in range(args.nvme_fill):
                    current = manager.metrics()
                    disk = sum(value for key, value in current.items()
                               if key.startswith("cm_bytes{") and 'tier="disk"' in key)
                    if disk >= quota // 2:
                        break
                    filler, _, _ = host_cache.conversation(
                        run, model, f"{nonce}-fill-{attempt}-{index}",
                        f"{label}-fill-{attempt}-{index}", TIER_SENTENCES[tier],
                    )
                    del filler
                    written = manager.metrics().get("cm_nvme_bytes_total",
                                                    0.0) - written_before
                    if written >= budget:
                        break
                filler_bytes += written
            # Clear the consumer's GPU prefix cache without publishing anything to the
            # manager. Published fillers would evict the target from the DRAM arena too,
            # which would make a DRAM row a disk row.
            replica.stop()
            replica.start()
        # The timed request is exactly the original single request: one per attempt, no
        # logprobs. Each seeded correctness request runs afterwards, outside the latency
        # statistics, and reads the prefix this request restored into the GPU cache.
        hits_before = manager.tier_hits() if manager is not None else {}
        fetched_before = host_cache.wait_quiescent(run, f"{label}-fetch-{attempt}-before",
                                                   args.timeout)
        result = isolation.stream(replica.server.url,
                                  host_cache.body(model, messages, ANSWER_TOKENS), args.timeout)
        fetched_after = host_cache.wait_quiescent(run, f"{label}-fetch-{attempt}-after",
                                                  args.timeout)
        hits_after = manager.tier_hits() if manager is not None else {}
        memory_hits = hits_after.get("memory", 0) - hits_before.get("memory", 0)
        disk_hits = hits_after.get("disk", 0) - hits_before.get("disk", 0)
        fetched = max(0.0, fetched_after.get("flare_host_cache_fetched_total", 0)
                      - fetched_before.get("flare_host_cache_fetched_total", 0))
        store_failed = (
            fetched_after.get("flare_host_cache_store_failed_total", 0)
            - fetched_before.get("flare_host_cache_store_failed_total", 0)
        )
        fetch_failed = (
            fetched_after.get("flare_host_cache_fetch_failed_total", 0)
            - fetched_before.get("flare_host_cache_fetch_failed_total", 0)
        )
        if store_failed > 0 or fetch_failed > 0:
            errors.append(
                f"attempt {attempt}: {store_failed:g} store / {fetch_failed:g} fetch failures"
            )
            continue
        if result["errors"]:
            errors.append(f"attempt {attempt}: {result['errors'][0]}")
            continue
        answer = tuple(result["tokens"])
        if not answer:
            errors.append(f"attempt {attempt}: empty answer")
            continue
        if result["reused"] is None or result["reused"] <= 0:
            errors.append(f"attempt {attempt}: no tokens reused")
            continue
        if mode == "dram" and (memory_hits <= 0 or fetched <= 0):
            errors.append(f"attempt {attempt}: no DRAM fetch for this request")
            continue
        if mode == "nvme" and (disk_hits <= 0 or fetched <= 0):
            errors.append(f"attempt {attempt}: no NVMe fetch for this request")
            continue
        verdicts = []
        for seed in seeds:
            # The requested seeds always run. Long-tier extras run only until the row has
            # enough compared positions for its mean to be meaningful.
            if (seed not in args.seeds and compared_positions >= args.long_positions):
                break
            correctness = oracle.stream(
                replica.server.url,
                oracle.correctness_body(
                    seeded_body(host_cache, model, messages, ANSWER_TOKENS, seed)),
                args.timeout)
            if correctness["errors"] or not correctness["tokens"]:
                reason = correctness["errors"][:1] or ["empty answer"]
                verdicts.append({"seed": seed, "matched": False, "evidence": reason})
                correctness_mismatches += 1
                errors.append(f"attempt {attempt} seed {seed}: correctness request failed: "
                              f"{reason}")
                continue
            if correctness["reused"] is None or correctness["reused"] <= 0:
                verdicts.append({"seed": seed, "matched": False,
                                 "evidence": ["reused no cached tokens"]})
                correctness_mismatches += 1
                errors.append(f"attempt {attempt} seed {seed}: correctness request reused "
                              "no cached tokens (the comparison would be cold against cold)")
                continue
            matched, evidence = oracle.agrees(expected[seed]["tokens"],
                                              expected[seed]["logprobs"],
                                              correctness["tokens"],
                                              correctness["logprobs"],
                                              limits=args.comparison_limits)
            compared_positions += evidence["compared"]
            top5_mismatches += evidence["top5_mismatches"]
            if evidence["mean_delta"] is not None:
                mean_deltas.append(evidence["mean_delta"])
                weighted_mean += evidence["mean_delta"] * evidence["compared"]
            if evidence["max_delta"] is not None and (max_delta is None
                                                      or evidence["max_delta"] > max_delta):
                max_delta = evidence["max_delta"]
            verdicts.append({"seed": seed, "matched": matched, "evidence": evidence})
            if not matched:
                correctness_mismatches += 1
                errors.append(f"attempt {attempt} seed {seed}: distribution differs from "
                              f"its cold recompute: {evidence['failures'][:1] or evidence}")
        observations.append({
            "ttft_ms": ttft_ms(result),
            "reused": result["reused"] or 0,
            "prompt_tokens": (result.get("usage") or {}).get("prompt_tokens", 0),
            "answer_tokens": len(answer),
            "correctness": verdicts,
            "memory_hits": memory_hits,
            "disk_hits": disk_hits,
            "fetched": fetched,
        })
    ttfts = [item["ttft_ms"] for item in observations if item["ttft_ms"] is not None]
    row_mean = weighted_mean / compared_positions if compared_positions else None
    if len(observations) != args.reps:
        errors.append("missing observations")
    manager_end = manager.metrics() if manager is not None else {}
    manager_counters = {}
    manager_gauges = {}
    for key, value in manager_end.items():
        if not key.startswith("cm_"):
            continue
        if "puts_total" in key or "gets_total" in key or "hits_total" in key:
            manager_counters[key] = value - manager_start.get(key, 0.0)
        elif "records" in key or "bytes" in key:
            manager_gauges[key] = value
    row = {
        "topology": topology,
        "mode": mode,
        "tier": tier,
        "sentences": TIER_SENTENCES[tier],
        "seeds": seeds,
        "reps": len(observations),
        "errors": errors,
        "compared_positions": compared_positions,
        "max_delta": max_delta,
        "mean_delta": row_mean,
        "fingerprint": fingerprint,
        "statistics": {"mean_delta": row_mean, "max_delta": max_delta,
                       "compared": compared_positions},
        "manager_counters": manager_counters,
        "manager_records": manager_gauges,
        "filler_bytes": filler_bytes,
        "top5_fraction": (top5_mismatches / compared_positions
                          if compared_positions else None),
        "correctness_mismatches": correctness_mismatches,
        "answer_tokens": len(expected[args.seeds[0]]["tokens"]),
        "fetched": sum(item["fetched"] for item in observations),
        "fetched_per_request": statistics.median(item["fetched"] for item in observations)
        if observations else None,
        "memory_hits": sum(item["memory_hits"] for item in observations),
        "disk_hits": sum(item["disk_hits"] for item in observations),
        "ttft_p50_ms": statistics.median(ttfts) if ttfts else None,
        "ttft_p99_ms": p99(ttfts) if ttfts else None,
        "reused_p50": statistics.median(item["reused"] for item in observations)
        if observations else None,
        "prompt_p50": statistics.median(item["prompt_tokens"] for item in observations)
        if observations else None,
    }
    row = apply_gates(row, mode, floor, native_row(facts, topology, tier),
                      args.long_positions, args.absolute_max)
    facts.append(row)
    print(f"{topology:8s} {mode:4s} {tier:5s} "
          f"p50 {row['ttft_p50_ms']} ms reused {row['reused_p50']} "
          f"answers {row['answer_tokens']} delta {row['max_delta']} "
          f"fetched {row['fetched']} "
          f"mem {row['memory_hits']} disk {row['disk_hits']} "
          f"{'PASS' if row['pass'] else 'FAIL'} ({row['reason']})")
    return row


def loaded(host_cache, isolation, args, replicas, model, manager_on, tag):
    """Sustained load: every request has a fresh long prefix and decodes LOAD_TOKENS.

    `tag` identifies the trial so prompt ids stay unique across every trial and side.
    """
    stop = time.monotonic() + args.load_seconds
    delivered = []
    ttfts = []
    errors = []

    def worker(index):
        count = 0
        serial = 0
        while time.monotonic() < stop:
            nonce = f"load-{tag}-{index}-{serial}"
            serial += 1
            messages = [{"role": "user", "content": host_cache.long_prompt(
                nonce, TIER_SENTENCES["8k"])}]
            result = isolation.stream(replicas[index % len(replicas)].server.url,
                                      host_cache.body(model, messages, args.load_tokens),
                                      args.timeout)
            if result["errors"]:
                errors.append(result["errors"])
                return
            count += len(result["core_ns"])
            sample = ttft_ms(result)
            if sample is not None:
                ttfts.append(sample)
        delivered.append(count)

    with ThreadPoolExecutor(max_workers=args.load_concurrency) as workers:
        futures = [workers.submit(worker, index) for index in range(args.load_concurrency)]
        for future in futures:
            future.result()
    if errors:
        raise AssertionError(f"load failed: {errors[:1]}")
    return {
        "manager_on": manager_on,
        "tokens": sum(delivered),
        "seconds": args.load_seconds,
        "tokens_per_second": sum(delivered) / args.load_seconds,
        "ttft_p50_ms": statistics.median(ttfts) if ttfts else None,
        "ttft_p99_ms": p99(ttfts) if ttfts else None,
    }


def planned_runs(args, rows) -> tuple[list[dict], list[dict]]:
    """Every manager command line, replica assignment, instance name and work directory."""
    runs = []
    metrics_port = 9300
    for row in rows:
        for mode in MODES:
            name = f"l3-{row['name']}-{mode}"
            base = args.workdir / name
            manager = None
            if mode != "gpu":
                socket = base / "manager.sock"
                root = base / "records"
                command = manager_command(args, name, mode, socket, root, metrics_port)
                manager = {
                    "name": name,
                    "mode": mode,
                    "command": command,
                    "socket": str(socket),
                    "root": str(root) if mode == "nvme" else None,
                    "metrics_port": metrics_port,
                    "workdir": str(base),
                    "log": str(base / "manager.log"),
                }
                metrics_port += 1
            replicas = [
                {
                    "binary": str(args.flare),
                    "package": str(replica["package"]),
                    "gpus": replica["gpus"],
                    "port": replica["port"],
                    "extra": (["--prefix-cache-manager", manager["socket"]] if manager else [])
                    + list(args.flare_arg),
                    "workdir": str(args.workdir),
                    "log": str(args.workdir / f"l3-{row['name']}-{mode}-{index}.log"),
                }
                for index, replica in enumerate(row["replicas"])
            ]
            runs.append({"topology": row["name"], "mode": mode, "manager": manager,
                         "replicas": replicas})
    offpath = []
    if not args.skip_offpath and args.main_flare is not None:
        for name in ("1xTP1", "1xTP4"):
            row = next(row for row in rows if row["name"] == name)
            replica = row["replicas"][0]
            for label, binary in [("main", args.main_flare), ("branch", args.flare)]:
                offpath.append({
                    "topology": name,
                    "label": label,
                    "binary": str(binary),
                    "package": str(replica["package"]),
                    "gpus": replica["gpus"],
                    "port": replica["port"],
                    "extra": list(args.flare_arg),
                    "log": str(args.workdir / f"l3-offpath-{name}-{label}.log"),
                })
    return runs, offpath


def write_through_plan(args, runs, rows) -> list[dict]:
    """Planned alternating trials: which mode's manager and replicas run, and where they log."""
    trials = []
    if args.skip_load:
        return trials
    for row in rows:
        for trial in range(args.write_through_trials):
            for manager_on, mode in ((True, "nvme"), (False, "gpu")):
                spec = planned_for(runs, row["name"], mode)
                prefix = f"l3-{row['name']}-wt{trial}-{'on' if manager_on else 'off'}"
                trials.append({
                    "topology": row["name"],
                    "trial": trial,
                    "manager_on": manager_on,
                    "manager": spec["manager"]["command"] if spec["manager"] else None,
                    "manager_name": spec["manager"]["name"] if spec["manager"] else None,
                    "manager_socket": spec["manager"]["socket"] if spec["manager"] else None,
                    "replicas": [
                        {
                            "port": replica["port"],
                            "gpus": replica["gpus"],
                            "extra": spec["replicas"][index]["extra"],
                            "log": str(args.workdir / f"{prefix}-{index}.log"),
                        }
                        for index, replica in enumerate(row["replicas"])
                    ],
                })
    return trials


def validate_plan(runs, offpath, trials) -> None:
    names = [run["manager"]["name"] for run in runs if run["manager"]]
    if len(names) != len(set(names)):
        raise SystemExit("duplicate manager instance names")
    ports = [run["manager"]["metrics_port"] for run in runs if run["manager"]]
    if len(ports) != len(set(ports)):
        raise SystemExit("duplicate manager metrics ports")
    for run in runs:
        replicas = run["replicas"]
        replica_ports = [replica["port"] for replica in replicas]
        if len(replica_ports) != len(set(replica_ports)):
            raise SystemExit(f"duplicate replica ports in {run['topology']}/{run['mode']}")
        gpus = [gpu for replica in replicas for gpu in replica["gpus"]]
        if len(gpus) != len(set(gpus)):
            raise SystemExit(f"duplicate GPU assignment in {run['topology']}/{run['mode']}")
        logs = [replica["log"] for replica in replicas]
        if run["manager"] is not None:
            logs.append(run["manager"]["log"])
        if len(logs) != len(set(logs)):
            raise SystemExit(f"duplicate log files in {run['topology']}/{run['mode']}")
        for path in logs:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
    for entry in offpath:
        Path(entry["log"]).parent.mkdir(parents=True, exist_ok=True)
    for trial in trials:
        ports = [replica["port"] for replica in trial["replicas"]]
        if len(ports) != len(set(ports)):
            raise SystemExit(f"duplicate replica ports in {trial['topology']} trial {trial['trial']}")
        gpus = [gpu for replica in trial["replicas"] for gpu in replica["gpus"]]
        if len(gpus) != len(set(gpus)):
            raise SystemExit(f"duplicate GPU assignment in {trial['topology']} trial {trial['trial']}")
        for replica in trial["replicas"]:
            Path(replica["log"]).parent.mkdir(parents=True, exist_ok=True)


def planned_for(runs, topology, mode) -> dict:
    return next(run for run in runs if run["topology"] == topology and run["mode"] == mode)


def start_run(host_cache, args, spec, prefix):
    """Start the manager (if any) and replicas of one planned run."""
    manager = None
    if spec["manager"] is not None:
        plan = spec["manager"]
        manager = Manager(args, plan["name"], spec["mode"], plan["metrics_port"],
                          plan["command"], Path(plan["socket"]),
                          Path(plan["root"]) if plan["root"] else Path())
        manager.start()
    replicas = [
        Replica(host_cache, args, args.flare, replica["package"], replica["gpus"],
                replica["port"], f"{prefix}-{index}", replica["extra"])
        for index, replica in enumerate(spec["replicas"])
    ]
    try:
        for replica in replicas:
            replica.start()
    except BaseException:
        for replica in replicas:
            replica.stop()
        if manager is not None:
            manager.stop()
        raise
    return manager, replicas


def stop_run(manager, replicas) -> None:
    for replica in replicas:
        replica.stop()
    if manager is not None:
        manager.stop()


def write_through(host_cache, isolation, args, runs, rows) -> list[dict]:
    """Alternating manager-on/off load trials per topology, gated by median cost."""
    results = []
    tolerance = args.write_through_tolerance
    for row in rows:
        on_samples = []
        off_samples = []
        for trial in range(args.write_through_trials):
            for manager_on, spec_mode, samples in (
                (True, "nvme", on_samples),
                (False, "gpu", off_samples),
            ):
                spec = planned_for(runs, row["name"], spec_mode)
                prefix = f"l3-{row['name']}-wt{trial}-{'on' if manager_on else 'off'}"
                if manager_on:
                    # A fresh durable directory per manager-on trial, so no trial reuses
                    # another's records.
                    root = str(args.workdir / prefix / "records")
                    command = list(spec["manager"]["command"])
                    command[command.index("--root") + 1] = root
                    spec = {**spec, "manager": {**spec["manager"], "command": command,
                                                "root": root}}
                manager, replicas = start_run(host_cache, args, spec, prefix)
                try:
                    samples.append(loaded(host_cache, isolation, args, replicas,
                                          replicas[0].server.model_id(), manager_on, prefix))
                finally:
                    stop_run(manager, replicas)
        on_tps = statistics.median(sample["tokens_per_second"] for sample in on_samples)
        off_tps = statistics.median(sample["tokens_per_second"] for sample in off_samples)
        on_ttfts = [sample["ttft_p50_ms"] for sample in on_samples
                    if sample["ttft_p50_ms"] is not None]
        off_ttfts = [sample["ttft_p50_ms"] for sample in off_samples
                     if sample["ttft_p50_ms"] is not None]
        on_ttft = statistics.median(on_ttfts) if on_ttfts else None
        off_ttft = statistics.median(off_ttfts) if off_ttfts else None
        tps_ok = on_tps >= off_tps * (1 - tolerance)
        ttft_ok = on_ttft is not None and off_ttft is not None and on_ttft <= off_ttft * (
            1 + tolerance
        )
        results.append({
            "topology": row["name"],
            "trials": args.write_through_trials,
            "tolerance": tolerance,
            "on_samples": on_samples,
            "off_samples": off_samples,
            "on_tokens_per_second": on_tps,
            "off_tokens_per_second": off_tps,
            "on_ttft_p50_ms": on_ttft,
            "off_ttft_p50_ms": off_ttft,
            "tps_ratio": on_tps / off_tps if off_tps else None,
            "ttft_ratio": on_ttft / off_ttft if on_ttft and off_ttft else None,
            "pass": bool(tps_ok and ttft_ok),
        })
        result = results[-1]
        print(f"{row['name']:8s} write-through on {on_tps:.1f} tok/s "
              f"off {off_tps:.1f} tok/s "
              f"ttft {on_ttft} vs {off_ttft} ms "
              f"{'PASS' if result['pass'] else 'FAIL'}")
    return results


def main() -> int:
    args = parse()
    rows = topologies(args)
    runs, offpath = planned_runs(args, rows)
    trials = write_through_plan(args, runs, rows)
    validate_plan(runs, offpath, trials)
    plan = {
        "label": args.label,
        "topologies": [
            {"name": row["name"], "gpus": row["gpus"],
             "replicas": [
                 {"gpus": replica["gpus"], "package": str(replica["package"]),
                  "port": replica["port"]}
                 for replica in row["replicas"]
             ]}
            for row in rows
        ],
        "tiers": args.tiers,
        "reps": args.reps,
        "seeds": args.seeds,
        "envelope": args.envelope,
        "modes": list(MODES),
        "answer_tokens": ANSWER_TOKENS,
        "nvme_memory": args.nvme_memory,
        "nvme_fill": args.nvme_fill,
        "write_through": not args.skip_load,
        "write_through_trials": args.write_through_trials,
        "write_through_tolerance": args.write_through_tolerance,
        "min_free": args.min_free,
        "load_concurrency": args.load_concurrency,
        "load_tokens": args.load_tokens,
        "load_seconds": args.load_seconds,
        "offpath": bool(offpath),
        "offpath_tolerance": args.offpath_tolerance,
        "runs": runs,
        "write_through_runs": trials,
        "offpath_runs": offpath,
    }
    if args.dry_run:
        print(json.dumps(plan, indent=1))
        return 0

    # A previous aborted run's manager records are stale here; the live ones are only ever
    # created inside this loop.
    for stale in args.workdir.glob("l3-*/records"):
        shutil.rmtree(stale, ignore_errors=True)
    host_cache, isolation = load_helpers(args.flare_root)
    facts: list[dict] = []
    offpath_results: dict = {}
    cold_noise: dict = {}
    write_through_results: list[dict] = []
    fingerprint = run_fingerprint(args)
    prior: dict = {}
    references: dict = {}
    reference_ids: dict = {}
    if args.resume and (args.workdir / "l3.json").exists():
        old = json.loads((args.workdir / "l3.json").read_text())
        for row in old.get("tiers", []):
            prior[(row["topology"], row["mode"], row["tier"])] = row
        cold_noise.update(old.get("cold_noise", {}))
        for key, reference in (old.get("references") or {}).items():
            references[key] = reference
            reference_ids[key] = reference_digest(reference)

    def write_result() -> None:
        """Keep l3.json current after every row, so a crash preserves the floors."""
        result = {"plan": plan, "tiers": facts, "write_through": write_through_results,
                  "offpath": offpath_results, "cold_noise": cold_noise,
                  "references": references}
        (args.workdir / "l3.json").write_text(json.dumps(result, indent=1))
    for row in ([] if args.only_write_through else rows):
        baselines: dict = {}
        for mode in MODES:
            spec = planned_for(runs, row["name"], mode)
            manager, replicas = start_run(host_cache, args, spec, f"l3-{row['name']}-{mode}")
            run = host_cache.run_for(replicas[0].server, args.workdir / f"l3-{row['name']}-{mode}",
                                     args.timeout)
            metrics.patch(run)
            model = replicas[0].server.model_id()
            try:
                for tier in args.tiers:
                    free = shutil.disk_usage(args.workdir).free
                    if free < args.min_free * (1 << 30):
                        raise SystemExit(
                            f"refusing to start {row['name']}/{mode}/{tier}: only "
                            f"{free / (1 << 30):.0f} GiB free (minimum {args.min_free})")
                    key = f"{row['name']}/{tier}"
                    if mode == "gpu":
                        prior_gpu = prior.get((row["name"], "gpu", tier))
                        stored = references.get(key) if args.resume else None
                        # Reuse the stored reference only when it is the very realisation
                        # the resumed native row was measured against; otherwise a restore
                        # would be judged against another process's cold answers.
                        reusable = (stored is not None and prior_gpu is not None
                                    and prior_gpu.get("pass") is True
                                    and prior_gpu.get("fingerprint") == {
                                        **fingerprint,
                                        "reference": reference_ids[key]})
                        if reusable:
                            messages, answers = load_reference(stored)
                        else:
                            messages, answers, noise = cold_baseline(
                                host_cache, args, replicas[0], run, row["name"], tier
                            )
                            references[key] = reference_record(messages, answers)
                            reference_ids[key] = reference_digest(references[key])
                            cold_noise[key] = {
                                str(seed): {"matched": matched,
                                            "max_delta": evidence["max_delta"],
                                            "mean_delta": evidence["mean_delta"],
                                            "top5_fraction": evidence["top5_fraction"]}
                                for seed, (matched, evidence) in noise.items()}
                        baselines[(row["name"], tier)] = (messages, answers)
                    cell_fingerprint = {**fingerprint, "reference": reference_ids[key]}
                    time_tier(host_cache, isolation, args, replicas[0], run, model, tier, mode,
                              manager, row["name"], baselines, facts, cold_noise,
                              cell_fingerprint, prior.get((row["name"], mode, tier)))
                    write_result()
            finally:
                stop_run(manager, replicas)
                # The row's results are in l3.json; its manager records are the bulk of the
                # disk use and are not needed again.
                shutil.rmtree(args.workdir / f"l3-{row['name']}-{mode}" / "records",
                              ignore_errors=True)
    if not args.skip_load:
        write_through_results = write_through(host_cache, isolation, args, runs, rows)
        write_result()
    if offpath:
        for name in ("1xTP1", "1xTP4"):
            row = next(row for row in rows if row["name"] == name)
            topology = offpath_results.setdefault(name, {})
            for label, binary in [("main", args.main_flare), ("branch", args.flare)]:
                replica = Replica(host_cache, args, binary, row["replicas"][0]["package"],
                                  row["replicas"][0]["gpus"], row["replicas"][0]["port"],
                                  f"l3-offpath-{name}-{label}", [])
                try:
                    replica.start()
                    topology[label] = loaded(host_cache, isolation, args, [replica],
                                             replica.server.model_id(), False,
                                             f"offpath-{name}-{label}")
                finally:
                    replica.stop()
                write_result()
            main_row = topology.get("main", {})
            branch_row = topology.get("branch", {})
            delta = args.offpath_tolerance
            pass_throughput = (
                main_row.get("tokens_per_second") and branch_row.get("tokens_per_second")
                and branch_row["tokens_per_second"] >= main_row["tokens_per_second"] * (1 - delta)
            )
            pass_ttft = (
                main_row.get("ttft_p50_ms") and branch_row.get("ttft_p50_ms")
                and branch_row["ttft_p50_ms"] <= main_row["ttft_p50_ms"] * (1 + delta)
            )
            topology["pass"] = bool(pass_throughput and pass_ttft)
            topology["tolerance"] = delta

    write_result()
    lines = ["| Topology | Mode | Tier | TTFT p50 ms | TTFT p99 ms | Reused tokens | "
             "Fetches/req | Mem hits | Disk hits | Answer tokens | Pass | Reason |",
             "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|"]
    for row in facts:
        lines.append(
            f"| {row.get('topology')} | {row.get('mode')} | {row.get('tier')} | "
            f"{row.get('ttft_p50_ms')} | {row.get('ttft_p99_ms')} | "
            f"{row.get('reused_p50')} | {row.get('fetched_per_request')} | "
            f"{row.get('memory_hits')} | {row.get('disk_hits')} | "
            f"{row.get('answer_tokens')} | "
            f"{'PASS' if row.get('pass') else 'FAIL'} | {row.get('reason')} |"
        )
    if write_through_results:
        lines += ["", "| Write-through | On tok/s | Off tok/s | On TTFT ms | Off TTFT ms | "
                  "Pass |", "|---|---:|---:|---:|---:|---|"]
        for row in write_through_results:
            lines.append(
                f"| {row['topology']} | {row['on_tokens_per_second']:.1f} | "
                f"{row['off_tokens_per_second']:.1f} | {row['on_ttft_p50_ms']} | "
                f"{row['off_ttft_p50_ms']} | {'PASS' if row['pass'] else 'FAIL'} |"
            )
    if offpath_results:
        lines += ["", "| Off-path | Tokens/s | TTFT p50 ms | Delta | Pass |",
                  "|---|---:|---:|---:|---|"]
        for name, topology in offpath_results.items():
            main_row = topology.get("main", {})
            branch_row = topology.get("branch", {})
            baseline = main_row.get("tokens_per_second") or 0
            delta = ((branch_row.get("tokens_per_second") or 0) / baseline - 1) if baseline else None
            lines.append(
                f"| {name} | {branch_row.get('tokens_per_second')} "
                f"(main {main_row.get('tokens_per_second')}) | "
                f"{branch_row.get('ttft_p50_ms')} (main {main_row.get('ttft_p50_ms')}) | "
                f"{delta} | {'PASS' if topology.get('pass') else 'FAIL'} |"
            )
    (args.workdir / "l3.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    failed = [row for row in facts if not row["pass"]]
    if failed:
        return 1
    if any(not row["pass"] for row in write_through_results):
        return 1
    if offpath_results and any(not topology.get("pass") for topology in offpath_results.values()):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

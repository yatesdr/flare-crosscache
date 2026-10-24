#!/usr/bin/env python3
"""L2: two-replica lifecycle against a real manager.

Publish on one replica, evict the GPU copy, restore from DRAM on the other, evict DRAM, restore
from NVMe, kill the publisher mid-copy, restart a replica gracefully, kill and restart the
manager, and restore again. Every fact request carries a fixed seed and is checked against the
same-seed cold recompute from a manager-off replica, sampled twice to record the run's cold-vs-cold
noise floor. Because sampling is not greedy (temperature 0 is clamped into the package's declared
range), a fixed seed fixes the distribution, not the sampled token: at every position whose context
is identical the cold and measured top-20 distributions are compared, and each fact is gated
against its own floor (mean within max(1.5 x floor, floor + 0.15), with an absolute
10-nat guard on any single difference; the maximum, top-5 fraction and exact tokens are
recorded but not gated). The fixed mean envelope is only a fallback when no floor was
measured.

The reference is computed by the harness, not hardcoded: the same messages and correctness
request, sent with the same seed to a replica with no manager whose GPU copy is evicted first.

The script is destructive on the assigned GPUs. Use --dry-run to validate paths and print the
plan without starting anything. Cluster helpers come from Flare's `qualify` package; pass its
repository directory with --flare-root.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import metrics
import oracle


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manager", type=Path, required=True, help="selene binary")
    parser.add_argument("--flare", type=Path, required=True, help="flare binary")
    parser.add_argument("--flare-root", type=Path, required=True,
                        help="Flare repository root containing the qualify package")
    parser.add_argument("--flare-arg", action="append", default=[],
                        help="extra argument for every replica's `flare run`; repeatable")
    parser.add_argument("--package", type=Path, action="append", required=True,
                        help="a TP1 model package for a replica; pass twice")
    parser.add_argument("--gpus", default="0,1", help="two GPU ids, comma separated")
    parser.add_argument("--ports", default="8031,8032", help="two replica HTTP ports")
    parser.add_argument("--test-root", type=Path, required=True,
                        help="Flare test root; no default")
    parser.add_argument("--workdir", type=Path, required=True, help="NVMe scratch for root/socket/logs")
    parser.add_argument("--memory", default="64M", help="manager DRAM, small enough to evict")
    parser.add_argument("--nvme-fill", type=int, default=None,
                        help="filler conversations before the NVMe restore; default scales "
                             "with --memory so the target leaves DRAM")
    parser.add_argument("--nvme", default="64G")
    parser.add_argument("--staging", default="256M")
    parser.add_argument("--metrics-port", type=int, default=9208)
    parser.add_argument("--sentences", type=int, default=2000, help="32k prompt by default")
    parser.add_argument("--nonce", default=None,
                        help="prompt nonce; default is time based, pass one to pair runs")
    parser.add_argument("--seeds", default="12345,12346,12347",
                        help="fixed request seeds per fact; each answer is compared with its "
                             "same-seed cold recompute")
    parser.add_argument("--oracle-envelope", default=None,
                        help="calibrated MEAN,MAX,TOP5[,ABSOLUTE] nats; without it the strict "
                             "per-position rule applies")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--startup-timeout", type=float, default=900)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if len(args.package) != 2:
        parser.error("pass exactly two --package values")
    args.memory_bytes = parse_size(args.memory)
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
    # Comparisons record statistics against the absolute guard only; each fact is then
    # gated against its own run's cold-vs-cold floor, with the fixed values as fallback.
    args.absolute_max = args.envelope["absolute_max"] or oracle.ABSOLUTE_MAX
    args.comparison_limits = oracle.envelope(absolute_max=args.absolute_max)
    args.fallback_mean = args.envelope["mean_delta"]
    args.fallback_max = args.envelope["max_delta"]
    args.gpus = [value.strip() for value in args.gpus.split(",")]
    args.ports = [int(value) for value in args.ports.split(",")]
    if len(args.gpus) != 2 or len(args.ports) != 2:
        parser.error("--gpus and --ports need exactly two values")
    for path in [args.manager, args.flare, *args.package]:
        if not path.exists():
            parser.error(f"missing path: {path}")
    if not (args.flare_root / "qualify" / "host_cache.py").exists():
        parser.error(f"{args.flare_root} does not contain qualify/host_cache.py")
    args.workdir.mkdir(parents=True, exist_ok=True)
    return args


def parse_size(text: str) -> int:
    """A byte size with a binary K, M, G or T suffix, as the manager accepts."""
    suffix = text[-1:].lower()
    multiplier = {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3, "t": 1024 ** 4}.get(suffix)
    if multiplier is None:
        return int(text)
    return int(text[:-1]) * multiplier


def load_helpers(root: Path):
    sys.path.insert(0, str(root))
    from qualify import host_cache  # noqa: PLC0415

    return host_cache


def seeded(host_cache, model, messages, seed) -> dict:
    """A completion request for these messages pinned to one request seed."""
    request = host_cache.body(model, messages)
    request["seed"] = seed
    return request


class Manager:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.name = "l2"
        self.socket = args.workdir / "manager.sock"
        self.root = args.workdir / "records"
        self.log = (args.workdir / "manager.log").open("a")
        self.process: subprocess.Popen | None = None

    def command(self) -> list[str]:
        return [
            str(self.args.manager),
            "--name", self.name,
            "--socket", str(self.socket),
            "--root", str(self.root),
            "--memory", self.args.memory,
            "--nvme", self.args.nvme,
            "--staging", self.args.staging,
            "--metrics", f"127.0.0.1:{self.args.metrics_port}",
        ]

    def start(self, wait: bool = True) -> None:
        self.process = subprocess.Popen(self.command(), stdout=self.log, stderr=subprocess.STDOUT)
        if wait:
            self.wait_ready()

    def wait_ready(self) -> None:
        deadline = time.monotonic() + 120
        while True:
            assert self.process is not None and self.process.poll() is None, "manager exited"
            try:
                with socket.socket(socket.AF_UNIX) as peer:
                    peer.connect(str(self.socket))
                return
            except OSError:
                assert time.monotonic() < deadline, "manager socket did not appear"
                time.sleep(0.1)

    def stop(self, sig: int = signal.SIGTERM) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(sig)
            self.process.wait(timeout=120)
        self.process = None

    def metrics(self) -> dict[str, float]:
        try:
            text = urllib.request.urlopen(
                f"http://127.0.0.1:{self.args.metrics_port}/metrics", timeout=2
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

    def nvme_bytes(self) -> float:
        return sum(value for name, value in self.metrics().items() if "nvme" in name and "bytes" in name)

    def tier_hits(self) -> dict[str, float]:
        hits: dict[str, float] = {}
        for name, value in self.metrics().items():
            if not name.startswith("cm_hits_total{"):
                continue
            for tier in ("memory", "disk"):
                if f'tier="{tier}"' in name:
                    hits[tier] = hits.get(tier, 0.0) + value
        return hits

    def close(self) -> None:
        self.stop()
        self.log.close()


class Replica:
    def __init__(self, host_cache, args, flare, package, gpu, port, label):
        self.host_cache = host_cache
        self.args = args
        self.flare = flare
        self.package = package
        self.gpu = gpu
        self.port = port
        self.label = label
        self.log_path = args.workdir / f"{label}.log"
        self.server = None
        self.run = None

    def start(self, manager_socket: Path | None) -> None:
        extra = (["--prefix-cache-manager", str(manager_socket)] if manager_socket else [])
        extra += list(self.args.flare_arg)
        self.server = self.host_cache.Server(
            self.flare, self.package, self.gpu, self.port, self.log_path, extra
        )
        self.server.wait_ready(self.args.startup_timeout)
        self.run = self.host_cache.run_for(self.server, self.args.workdir / self.label, self.args.timeout)
        metrics.patch(self.run)
        self.model = self.server.model_id()

    def stop(self, sig: int = signal.SIGTERM) -> None:
        if self.server is not None:
            self.server.stop()
        self.server = None
        self.run = None


def check_fact(host_cache, args, replica, messages, tokens, label, facts, cold, floor):
    """Every fixed seed must reproduce its same-seed cold distributions where contexts match.

    A failing seed is recorded and the remaining seeds and facts still run, so one run
    reports every fact's verdict; the run fails at the end if any did.
    """
    observations = []
    failures = []
    for seed in args.seeds:
        before = host_cache.wait_quiescent(replica.run, f"{label}-{seed}-before", args.timeout)
        result = oracle.stream(replica.server.url,
                               oracle.correctness_body(seeded(host_cache, replica.model,
                                                              messages, seed)),
                               args.timeout)
        after = host_cache.wait_quiescent(replica.run, f"{label}-{seed}-after", args.timeout)
        delta = host_cache.metric_delta(before, after)
        observation = {"seed": seed, "reused": result["reused"] or 0,
                       "fetched": delta.get("flare_host_cache_fetched_total", 0)}
        if result["errors"] or not result["tokens"]:
            reason = result["errors"][:1] or ["empty answer"]
            failures.append(f"{label} seed {seed}: {reason}")
            observations.append({**observation, "matched": False, "evidence": reason})
            continue
        answer = tuple(result["tokens"])
        cold_tokens, cold_logprobs = cold[seed]
        matched, evidence = oracle.agrees(cold_tokens, cold_logprobs, answer,
                                          result["logprobs"],
                                          limits=args.comparison_limits)
        observations.append({**observation, "answer_tokens": len(answer),
                             "tokens": list(answer), "matched": matched,
                             "evidence": evidence})
        if not matched:
            failures.append(f"{label} seed {seed}: distribution differs from the "
                            f"same-seed cold recompute: "
                            f"{evidence['failures'][:1] or evidence}")
    compared = sum(o["evidence"].get("compared", 0) for o in observations
                   if isinstance(o.get("evidence"), dict))
    weighted = sum(o["evidence"]["mean_delta"] * o["evidence"]["compared"]
                   for o in observations
                   if isinstance(o.get("evidence"), dict)
                   and o["evidence"].get("mean_delta") is not None)
    maximums = [o["evidence"]["max_delta"] for o in observations
                if isinstance(o.get("evidence"), dict)
                and o["evidence"].get("max_delta") is not None]
    mean = weighted / compared if compared else None
    maximum = max(maximums) if maximums else None
    passed, gate = oracle.gate(mean, maximum, floor[0], floor[1],
                               absolute_max=args.absolute_max,
                               fallback_mean=args.fallback_mean,
                               fallback_max=args.fallback_max)
    if not passed:
        failures.extend(gate["failures"])
    fact = {
        "label": label,
        "tokens": tokens,
        "reused": observations[0]["reused"] if observations else 0,
        "fetched": observations[0]["fetched"] if observations else 0,
        "observations": observations,
        "statistics": {"mean_delta": mean, "max_delta": maximum,
                       "compared": compared, "gate": gate},
        "passed": not failures,
        "failures": failures,
    }
    facts.append(fact)
    return fact


def cold_reference(host_cache, args, replica, nonce, messages) -> dict:
    """The same-seed cold recompute: manager off, GPU copy evicted before each sample.

    The cold answer also records per-position top-k generation logprobs, so a restored
    answer that diverges only at a near tie can be told apart from a real defect. A second
    cold sample per seed gives the cold-vs-cold noise floor under the same criterion; it is
    recorded, not used to pass or fail this run.
    """
    replica.stop()
    try:
        replica.start(None)
        answers = {}
        noise = {}
        for seed in args.seeds:
            samples = []
            for attempt in range(2):
                host_cache.evict_everything(
                    replica.run, replica.model, nonce, f"cold-{seed}-{attempt}", host=False)
                result = oracle.stream(
                    replica.server.url,
                    oracle.correctness_body(
                        seeded(host_cache, replica.model, messages, seed)),
                    args.timeout)
                if result["errors"] or not result["tokens"]:
                    raise AssertionError(
                        f"cold seed {seed} sample {attempt}: "
                        f"{result['errors'][:1] or 'empty answer'}")
                samples.append((tuple(result["tokens"]), result["logprobs"]))
            answers[seed] = samples[0]
            noise[seed] = oracle.agrees(samples[0][0], samples[0][1],
                                        samples[1][0], samples[1][1],
                                        limits=args.comparison_limits)
        return answers, noise
    finally:
        replica.stop()
        replica.start(args.workdir / "manager.sock")


def main() -> int:
    args = parse()
    host_cache = load_helpers(args.flare_root)
    manager = Manager(args)
    replicas = [
        Replica(host_cache, args, args.flare, args.package[index], args.gpus[index],
                args.ports[index], f"replica-{index}")
        for index in range(2)
    ]
    plan = {
        "manager": manager.command(),
        "replicas": [
            [str(args.flare), "run", str(args.package[index]),
             "--prefix-cache-manager", str(manager.socket), *args.flare_arg]
            for index in range(2)
        ],
        "sentences": args.sentences,
        "memory_bytes": args.memory_bytes,
        "nvme_fill": (args.nvme_fill if args.nvme_fill is not None
                      else max(64, 64 * ((args.memory_bytes + (1 << 30) - 1) // (1 << 30)))),
        "seeds": args.seeds,
        "envelope": args.envelope,
        "facts": [
            "publish-a", "gpu-evict-a", "dram-restore-b", "dram-evict", "nvme-restore-b",
            "publisher-sigkill", "survivor-restore", "graceful-restart", "manager-sigkill-restore",
        ],
    }
    if args.dry_run:
        print(json.dumps(plan, indent=1))
        return 0

    facts: list[dict] = []
    errors: list[BaseException] = []
    try:
        manager.start()
        for replica in replicas:
            replica.start(manager.socket)
        nonce = args.nonce or f"l2-{int(time.time())}"
        a, b = replicas
        messages, _, tokens = host_cache.conversation(
            a.run, a.model, nonce, "publish", args.sentences
        )
        deadline = time.monotonic() + 120
        while host_cache.wait_quiescent(a.run, "durable", 120).get(
            "flare_host_cache_stored_total", 0
        ) == 0:
            assert time.monotonic() < deadline, "publish was not stored"
            time.sleep(0.01)
        facts.append({"label": "publish-a", "tokens": tokens})

        # Clear the publisher's GPU copy by restarting it. Published fillers here would also
        # evict the target from the manager's DRAM arena, turning the DRAM fact into a disk
        # fact; the earlier stored_total wait already proves the publication reached the host.
        a.stop()
        a.start(manager.socket)
        cold, cold_noise = cold_reference(host_cache, args, b, nonce, messages)
        floor_means = [evidence["mean_delta"] for _, evidence in cold_noise.values()
                       if isinstance(evidence, dict)
                       and evidence.get("mean_delta") is not None]
        floor_maxes = [evidence["max_delta"] for _, evidence in cold_noise.values()
                       if isinstance(evidence, dict)
                       and evidence.get("max_delta") is not None]
        floor = (max(floor_means) if floor_means else None,
                 max(floor_maxes) if floor_maxes else None)
        hits_before = manager.tier_hits()
        check_fact(host_cache, args, b, messages, tokens, "dram-restore-b", facts,
                   cold, floor)
        hits_after = manager.tier_hits()
        facts[-1]["tier_delta"] = {
            tier: hits_after.get(tier, 0) - hits_before.get(tier, 0)
            for tier in ("memory", "disk")}

        # Fill the DRAM arena so the target leaves DRAM while NVMe keeps it. The number of
        # fillers scales with the arena: 64 fillers evict a 64 MiB arena, but a multi-gigabyte
        # arena needs proportionally more before the target is displaced.
        gib = (args.memory_bytes + (1 << 30) - 1) // (1 << 30)
        fillers = args.nvme_fill if args.nvme_fill is not None else max(64, 64 * gib)
        for index in range(fillers):
            filler, _, _ = host_cache.conversation(
                b.run, b.model, f"{nonce}-fill-{index}", f"fill-{index}", args.sentences
            )
            del filler
        hits_before = manager.tier_hits()
        fact = check_fact(host_cache, args, b, messages, tokens, "nvme-restore-b",
                          facts, cold, floor)
        # The DRAM copy must really be gone: the restore has to read the record from NVMe and
        # must not hit a memory copy. Manager NVMe bytes are not a usable check here, because
        # eviction during the fetch legitimately lowers them.
        hits_after = manager.tier_hits()
        memory_hits = hits_after.get("memory", 0) - hits_before.get("memory", 0)
        disk_hits = hits_after.get("disk", 0) - hits_before.get("disk", 0)
        if not (fact["reused"] > 0 and fact["fetched"] > 0):
            fact["passed"] = False
            fact["failures"].append("the NVMe restore did not hit the host cache")
        if not (disk_hits > 0 and memory_hits == 0):
            fact["passed"] = False
            fact["failures"].append(
                "the NVMe restore did not read from NVMe "
                f"(memory hits {memory_hits}, disk hits {disk_hits})")

        # Kill the publisher while another copy is being published.
        a.stop()
        a.start(manager.socket)
        check_fact(host_cache, args, b, messages, tokens, "survivor-restore", facts,
                   cold, floor)

        b.stop()
        b.start(manager.socket)
        check_fact(host_cache, args, b, messages, tokens, "graceful-restart", facts,
                   cold, floor)

        manager.stop(signal.SIGKILL)
        manager.start()
        time.sleep(2)
        check_fact(host_cache, args, b, messages, tokens, "manager-sigkill-restore",
                   facts, cold, floor)
        failed = [fact["label"] for fact in facts if not fact.get("passed", True)]
        result = {"plan": plan, "facts": facts,
                  "cold": {str(seed): list(cold_tokens)
                           for seed, (cold_tokens, _) in cold.items()},
                  "cold_noise": {str(seed): {"matched": matched, **evidence}
                                 for seed, (matched, evidence) in cold_noise.items()},
                  "cold_floor": {"mean_delta": floor[0], "max_delta": floor[1]},
                  "manager_metrics": manager.metrics(),
                  "pass": not failed}
        (args.workdir / "l2.json").write_text(json.dumps(result, indent=1))
        print(json.dumps({"facts": [fact["label"] for fact in facts],
                          "failed": failed,
                          "cold_noise": {str(seed): {"matched": matched,
                                                     "max_delta": evidence["max_delta"],
                                                     "mean_delta": evidence["mean_delta"],
                                                     "top5_fraction": evidence["top5_fraction"]}
                                         for seed, (matched, evidence)
                                         in cold_noise.items()}}, indent=1))
        if failed:
            raise AssertionError(f"{len(failed)} fact(s) failed: {failed}")
        return 0
    except BaseException as error:  # noqa: BLE001 - reported after cleanup
        errors.append(error)
        raise
    finally:
        for replica in replicas:
            try:
                replica.stop()
            except BaseException as error:  # noqa: BLE001
                errors.append(error)
        manager.close()
        if errors:
            raise ExceptionGroup("l2 cleanup failed", errors)


if __name__ == "__main__":
    sys.exit(main())

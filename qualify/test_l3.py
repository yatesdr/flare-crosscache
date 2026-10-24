"""Offline tests for the L3 resume fingerprint.

Run with `python3 qualify/test_l3.py`. No GPU or model is used.
"""

from __future__ import annotations

import copy
import json
import unittest
from types import SimpleNamespace

import l3


def base_args() -> SimpleNamespace:
    return SimpleNamespace(
        flare_arg=["--publisher-keys", "/keys.json", "--embedding-table-storage", "ram"],
        memory="16G",
        nvme_memory="16M",
        nvme_fill=32,
        staging="512M",
        nvme="64G",
        gpus=["0", "1", "2", "3"],
        tiers=["8k", "32k"],
        seeds=[12345, 12346, 12347],
        long_seeds=12,
        long_positions=200,
        reps=5,
        tp1_package=["/pkg/a1", "/pkg/a2", "/pkg/a3", "/pkg/a4"],
        tp2_package=["/pkg/tp2a", "/pkg/tp2b"],
        tp4_package=["/pkg/tp4"],
        envelope={"mean_delta": 0.66, "max_delta": 4.8, "top5_fraction": 1.0,
                  "absolute_max": 10.0},
    )


class FingerprintTests(unittest.TestCase):
    def test_unchanged_settings_match(self):
        self.assertEqual(l3.fingerprint_settings(base_args()),
                         l3.fingerprint_settings(base_args()))

    def test_every_setting_changes_the_fingerprint(self):
        changes = {
            "flare_arg": lambda args: args.flare_arg.append("--mtp-draft-tokens"),
            "memory": lambda args: setattr(args, "memory", "8G"),
            "nvme_memory": lambda args: setattr(args, "nvme_memory", "8M"),
            "nvme_fill": lambda args: setattr(args, "nvme_fill", 33),
            "staging": lambda args: setattr(args, "staging", "1G"),
            "nvme": lambda args: setattr(args, "nvme", "128G"),
            "gpus": lambda args: args.gpus.reverse(),
            "tiers": lambda args: args.tiers.reverse(),
            "seeds": lambda args: args.seeds.reverse(),
            "long_seeds": lambda args: setattr(args, "long_seeds", 8),
            "long_positions": lambda args: setattr(args, "long_positions", 300),
            "reps": lambda args: setattr(args, "reps", 6),
            "tp1_package": lambda args: args.tp1_package.reverse(),
            "tp2_package": lambda args: args.tp2_package.reverse(),
            "tp4_package": lambda args: args.tp4_package.append("/pkg/other"),
            "envelope": lambda args: args.envelope.__setitem__("mean_delta", 0.7),
        }
        original = l3.fingerprint_settings(base_args())
        for name, change in changes.items():
            with self.subTest(setting=name):
                args = copy.deepcopy(base_args())
                change(args)
                self.assertNotEqual(original, l3.fingerprint_settings(args), name)

    def test_package_order_matters(self):
        first = base_args()
        first.tp1_package = ["/pkg/a", "/pkg/b"]
        second = base_args()
        second.tp1_package = ["/pkg/b", "/pkg/a"]
        self.assertNotEqual(l3.fingerprint_settings(first),
                            l3.fingerprint_settings(second))


class PooledFloorTests(unittest.TestCase):
    def test_pools_every_seed_sample(self):
        noise = {
            "1": {"mean_delta": 0.2, "max_delta": 1.0},
            "2": {"mean_delta": 0.4, "max_delta": 3.0},
            "3": {"mean_delta": 0.6, "max_delta": 2.0},
        }
        floor = l3.pooled_floor(noise)
        self.assertAlmostEqual(floor["mean_delta"], 0.4)
        self.assertEqual(floor["max_delta"], 3.0)
        self.assertEqual(floor["samples"], 3)
        # The worst seed is recorded, not used as the floor.
        self.assertEqual(floor["seed_max_mean"], 0.6)

    def test_one_sample_is_the_floor(self):
        floor = l3.pooled_floor({"7": {"mean_delta": 0.33, "max_delta": 1.1}})
        self.assertAlmostEqual(floor["mean_delta"], 0.33)
        self.assertEqual(floor["samples"], 1)

    def test_no_usable_sample_has_no_floor(self):
        self.assertIsNone(l3.pooled_floor({}))
        self.assertIsNone(l3.pooled_floor({"7": {"max_delta": 1.0}}))
        self.assertIsNone(l3.pooled_floor({"7": None}))


class SeedScheduleTests(unittest.TestCase):
    def test_short_tiers_keep_the_requested_seeds(self):
        self.assertEqual(l3.seed_schedule(base_args(), "8k"), [12345, 12346, 12347])
        self.assertEqual(l3.seed_schedule(base_args(), "32k"), [12345, 12346, 12347])

    def test_long_tiers_add_seeds_up_to_the_cap(self):
        for tier in ("100k", "262k"):
            with self.subTest(tier=tier):
                schedule = l3.seed_schedule(base_args(), tier)
                self.assertEqual(len(schedule), 12)
                self.assertEqual(schedule[:3], [12345, 12346, 12347])
                self.assertEqual(len(set(schedule)), 12)

    def test_requested_seeds_are_never_duplicated(self):
        args = base_args()
        args.seeds = [12345, 12348]
        schedule = l3.seed_schedule(args, "100k")
        self.assertEqual(schedule[:2], [12345, 12348])
        self.assertEqual(len(set(schedule)), len(schedule))
        self.assertEqual(len(schedule), 12)

    def test_the_long_tier_cap_is_enforced(self):
        args = base_args()
        args.seeds = list(range(14000, 14015))
        args.long_seeds = 12
        schedule = l3.seed_schedule(args, "262k")
        self.assertEqual(len(schedule), l3.MAX_LONG_SEEDS)
        self.assertEqual(schedule, args.seeds[:l3.MAX_LONG_SEEDS])

    def test_short_tiers_are_not_capped(self):
        args = base_args()
        args.seeds = list(range(14000, 14015))
        self.assertEqual(l3.seed_schedule(args, "8k"), args.seeds)

    def test_a_long_seeds_setting_above_the_cap_is_clamped(self):
        args = base_args()
        args.long_seeds = 30
        self.assertEqual(len(l3.seed_schedule(args, "100k")), l3.MAX_LONG_SEEDS)

    def test_the_schedule_is_deterministic(self):
        self.assertEqual(l3.seed_schedule(base_args(), "100k"),
                         l3.seed_schedule(base_args(), "100k"))

    def test_the_fingerprint_carries_the_schedule(self):
        settings = l3.fingerprint_settings(base_args())
        self.assertEqual(settings["long_seed_cap"], 12)
        self.assertEqual(settings["long_positions"], 200)
        self.assertEqual(len(settings["long_seed_schedule"]["100k"]), 12)
        changed = base_args()
        changed.long_seeds = 6
        self.assertNotEqual(l3.fingerprint_settings(changed)["long_seed_schedule"],
                            settings["long_seed_schedule"])


class ApplyGatesTests(unittest.TestCase):
    COLD = {"mean_delta": 0.466, "max_delta": 2.7, "samples": 3}

    def row(self, mean, maximum=1.0, errors=None):
        return {
            "mean_delta": mean,
            "max_delta": maximum,
            "errors": list(errors or []),
            "pass": True,
            "reason": "cache hit on every request",
            "statistics": {"mean_delta": mean, "max_delta": maximum},
        }

    def gates(self, row, mode, native=None, cold=None, minimum=40):
        return l3.apply_gates(row, mode, self.COLD if cold is None else cold, native,
                              minimum, 10.0)

    def native(self, mean, maximum=1.5, **fields):
        row = {"topology": "1xTP1", "mode": "gpu", "tier": "100k", "pass": True,
               "errors": [], "mean_delta": mean, "max_delta": maximum,
               "compared_positions": 300, "correctness_mismatches": 0,
               "reused_p50": 107564, "prompt_p50": 107564}
        row.update(fields)
        return row

    def test_gpu_reuse_is_an_observation_not_a_gate(self):
        result = self.gates(self.row(0.958), "gpu")
        # Well above the cold floor, but that is Flare's numerics, not a selene failure.
        self.assertTrue(result["pass"])
        self.assertEqual(result["errors"], [])
        self.assertIsNone(result["statistics"]["gate"])
        self.assertFalse(result["statistics"]["flare_reuse"]["passed"])
        self.assertEqual(result["statistics"]["cold_floor"], self.COLD)

    def test_gpu_row_without_statistics_fails(self):
        result = self.gates(self.row(None, maximum=None), "gpu")
        self.assertFalse(result["pass"])
        self.assertIn("no distribution statistics", result["errors"])

    def test_restore_is_gated_against_native_reuse(self):
        # The 100k shape: the restore is within the native row's envelope even though both
        # are far above the cold floor.
        result = self.gates(self.row(0.927), "dram", self.native(0.958))
        self.assertTrue(result["pass"])
        gate = result["statistics"]["gate"]
        self.assertAlmostEqual(gate["mean_limit"], 1.437)
        self.assertEqual(result["statistics"]["native_reference"]["mean_delta"], 0.958)

    def test_restore_beyond_the_native_envelope_fails(self):
        result = self.gates(self.row(0.9), "dram", self.native(0.2))
        self.assertFalse(result["pass"])
        self.assertTrue(any("mean_delta" in error for error in result["errors"]))

    def test_a_restore_requires_a_valid_native_reference(self):
        for label, native in (
            ("missing", None),
            ("failed", dict(self.native(0.5), **{"pass": False})),
            ("error", dict(self.native(0.5), errors=["boom"])),
            ("too few positions", self.native(0.5, compared_positions=10)),
            ("a mismatch", self.native(0.5, correctness_mismatches=1)),
            ("incomplete reuse", self.native(0.5, reused_p50=1000, prompt_p50=2000)),
            ("no statistics", self.native(None, maximum=None)),
        ):
            with self.subTest(reference=label):
                result = self.gates(self.row(0.5), "dram", native)
                self.assertFalse(result["pass"])
                self.assertIn("no valid native reference", result["errors"])
                self.assertIsNone(result["statistics"]["native_reference"])

    def test_there_is_no_fixed_fallback(self):
        # 0.5 nats would pass the old 0.66 fallback; without a native row it must fail.
        result = self.gates(self.row(0.5), "nvme", None)
        self.assertFalse(result["pass"])

    def test_the_absolute_guard_still_applies(self):
        result = self.gates(self.row(0.1, maximum=11.0), "dram", self.native(1.0))
        self.assertFalse(result["pass"])

    def test_non_gate_failures_survive_and_stay_failures(self):
        failure = "attempt 0 seed 1: distribution differs from its cold recompute: x"
        result = self.gates(self.row(0.1, errors=[failure]), "dram", self.native(0.1))
        self.assertFalse(result["pass"])
        self.assertIn(failure, result["errors"])

    def test_the_row_is_not_mutated(self):
        row = self.row(0.958)
        before = copy.deepcopy(row)
        self.gates(row, "gpu")
        self.assertEqual(row, before)

    def test_native_row_lookup_uses_topology_and_tier(self):
        facts = [self.native(0.9), {"topology": "1xTP1", "mode": "gpu", "tier": "32k",
                                    "mean_delta": 0.1}]
        self.assertIs(l3.native_row(facts, "1xTP1", "100k"), facts[0])
        self.assertIsNone(l3.native_row(facts, "1xTP2", "100k"))


class FillerBudgetTests(unittest.TestCase):
    def test_sizes_use_binary_suffixes(self):
        self.assertEqual(l3.parse_size("16G"), 16 * 1024 ** 3)
        self.assertEqual(l3.parse_size("512M"), 512 * 1024 ** 2)
        self.assertEqual(l3.parse_size("1024"), 1024)
        self.assertEqual(l3.parse_size("1T"), 1024 ** 4)


class PublishWaitTests(unittest.TestCase):
    class Run:
        def __init__(self, values):
            self.values = list(values)
            self.labels = []

        def metrics(self, label=None):
            self.labels.append(label)
            value = self.values.pop(0) if len(self.values) > 1 else self.values[0]
            return {"flare_host_cache_stored_total": value}

    class HostCache:
        def wait_quiescent(self, run, label, timeout):
            return {}

    def wait(self, run, before, timeout=0.2):
        l3.wait_published(self.HostCache(), run, "l3-test", timeout, before)
        return run

    def test_a_store_that_completed_before_the_wait_counts(self):
        # The regression: the counter is captured before the publish, so a store that lands
        # before wait_published runs is seen immediately instead of timing out.
        run = self.wait(self.Run([6]), before=5)
        self.assertEqual(len(run.labels), 1)

    def test_the_wait_accepts_a_later_store(self):
        run = self.wait(self.Run([5, 5, 6]), before=5)
        self.assertEqual(len(run.labels), 3)

    def test_an_unstored_publish_times_out(self):
        with self.assertRaises(AssertionError):
            self.wait(self.Run([5]), before=5, timeout=0.05)


class ReferenceTests(unittest.TestCase):
    MESSAGES = [{"role": "user", "content": "the prompt"}]
    ANSWERS = {12345: {"tokens": [1, 2, 3],
                       "logprobs": [{"token_id": "1", "logprob": -0.1}]}}

    def test_a_record_roundtrips_through_json(self):
        record = l3.reference_record(self.MESSAGES, self.ANSWERS)
        messages, answers = l3.load_reference(json.loads(json.dumps(record)))
        self.assertEqual(messages, self.MESSAGES)
        self.assertEqual(answers[12345]["tokens"], [1, 2, 3])
        self.assertEqual(record["prompt_sha256"], l3.prompt_digest(self.MESSAGES))

    def test_the_digest_identifies_the_realisation(self):
        first = l3.reference_record(self.MESSAGES, self.ANSWERS)
        same = l3.reference_record(self.MESSAGES, self.ANSWERS)
        other = l3.reference_record(
            self.MESSAGES, {12345: {"tokens": [1, 2, 4],
                                    "logprobs": self.ANSWERS[12345]["logprobs"]}})
        self.assertEqual(l3.reference_digest(first), l3.reference_digest(same))
        self.assertNotEqual(l3.reference_digest(first), l3.reference_digest(other))

    def test_different_prompts_have_different_digests(self):
        self.assertNotEqual(l3.prompt_digest(self.MESSAGES),
                            l3.prompt_digest([{"role": "user", "content": "other"}]))


class FailedRowTests(unittest.TestCase):
    def test_a_failed_row_carries_every_rendered_field(self):
        row = l3.failed_row("1xTP1", "nvme", "100k", "publish failed: TimeoutError: drain")
        for field in ("topology", "mode", "tier", "ttft_p50_ms", "ttft_p99_ms",
                      "reused_p50", "fetched_per_request", "memory_hits", "disk_hits",
                      "answer_tokens", "pass", "reason", "errors", "compared_positions",
                      "statistics", "manager_counters", "manager_records"):
            self.assertIn(field, row)
        self.assertFalse(row["pass"])
        self.assertEqual(row["errors"], [row["reason"]])
        self.assertEqual(row["compared_positions"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

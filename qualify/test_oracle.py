"""Offline tests for the L2/L3 answer oracle.

Run with `python3 qualify/test_oracle.py`. The HTTP connection is mocked, so these exercise
the stream parser and the distribution comparison without a server or GPU.
"""

from __future__ import annotations

import json
import math
import unittest
from unittest import mock

import oracle


def alternatives(count: int = oracle.TOP_LOGPROBS, start: int = 0, base: float = -0.1):
    return [(start + index, base - 0.05 * index) for index in range(count)]


def entry(token: int, top=None, own: float = -0.1, jitter: float = 0.0) -> dict:
    top = alternatives() if top is None else top
    return {
        "token": f"token_id:{token}",
        "logprob": own,
        "top_logprobs": [{"token": f"token_id:{identifier}", "logprob": logprob + jitter}
                         for identifier, logprob in top],
    }


def observation(token: int, logprob: dict | None, finish=None, reused: int = 0) -> dict:
    choice = {"index": 0, "delta": {}, "finish_reason": finish}
    if token is not None:
        choice["token_ids"] = [token]
    if logprob is not None:
        choice["logprobs"] = {"content": [logprob]}
    event = {"choices": [choice]}
    if reused:
        event["reused_prompt_tokens"] = reused
    return event


def sse(event: dict | None = None, done: bool = False) -> list[bytes]:
    """The wire lines of one server-sent event."""
    if done:
        return [b"data: [DONE]\n", b"\n"]
    assert event is not None
    return [b"data: " + json.dumps(event).encode() + b"\n", b"\n"]


class FakeResponse:
    def __init__(self, lines: list[bytes], status: int = 200):
        self.status = status
        self.lines = list(lines)

    def readline(self, _limit: int) -> bytes:
        return self.lines.pop(0) if self.lines else b""

    def read(self, _limit: int) -> bytes:
        return b"error body"


class FakeConnection:
    response: FakeResponse

    def __init__(self, _host, _port, timeout):
        self.timeout = timeout
        self.sent = None

    def request(self, _method, _path, body=None, headers=None):
        self.sent = json.loads(body)
        assert headers["Accept"] == "text/event-stream"

    def getresponse(self) -> FakeResponse:
        return FakeConnection.response

    def close(self) -> None:
        pass


def stream(events: list[list[bytes]], request: dict | None = None) -> dict:
    lines = [line for event in events for line in event]
    FakeConnection.response = FakeResponse(lines)
    request = {"logprobs": True, "top_logprobs": oracle.TOP_LOGPROBS} if request is None else request
    with mock.patch("http.client.HTTPConnection", FakeConnection):
        return oracle.stream("http://127.0.0.1:8000", request, 10)


class StreamTests(unittest.TestCase):
    def test_complete_stream_is_accepted(self):
        result = stream([
            sse(observation(1, entry(1), reused=5)),
            sse(observation(2, entry(2))),
            sse(observation(None, None, finish="length")),
            sse(done=True),
        ])
        self.assertEqual(result["tokens"], [1, 2])
        self.assertEqual(len(result["logprobs"]), 2)
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["finish_reason"], "length")
        self.assertEqual(result["reused"], 5)

    def test_eof_without_done_or_finish_reason_fails(self):
        result = stream([sse(observation(1, entry(1)))])
        self.assertTrue(any("without [DONE]" in error for error in result["errors"]))
        self.assertTrue(any("finish reason" in error for error in result["errors"]))

    def test_done_without_finish_reason_fails(self):
        result = stream([sse(observation(1, entry(1))), sse(done=True)])
        self.assertTrue(any("finish reason" in error for error in result["errors"]))
        self.assertFalse(any("without [DONE]" in error for error in result["errors"]))

    def test_missing_logprob_entry_fails(self):
        result = stream([
            sse(observation(1, entry(1))),
            sse(observation(2, None)),
            sse(observation(None, None, finish="length")),
            sse(done=True),
        ])
        self.assertTrue(any("logprob entries" in error for error in result["errors"]))


class ComparisonTests(unittest.TestCase):
    def test_repeated_tokens_are_compared_by_position(self):
        first, second = entry(1), entry(1, jitter=0.04)
        # The entries belong to positions 0 and 1 of an answer with two equal token ids.
        ok, evidence = oracle.agrees([1, 1], [first, second], [1, 1], [first, second])
        self.assertTrue(ok, evidence)
        self.assertEqual(evidence["compared"], 2)

    def test_missing_entry_is_rejected_not_shifted(self):
        # Only the second position's logprob was captured; identity search would have
        # reported it at position zero and compared the wrong distribution.
        ok, evidence = oracle.agrees([1, 1], [entry(1)], [2, 2], [entry(2), entry(2)])
        self.assertFalse(ok)
        self.assertIn("do not match", evidence["failures"][0]["reason"])

    def test_sampled_token_divergence_under_matching_distributions_is_benign(self):
        cold = [entry(1), entry(2)]
        measured = [entry(1), entry(3)]
        ok, evidence = oracle.agrees([1, 2], cold, [1, 3], measured)
        self.assertTrue(ok, evidence)
        self.assertEqual(evidence["divergence"], 1)
        self.assertEqual(evidence["compared"], 2)

    def test_shifted_entries_are_rejected(self):
        shifted = [entry(2), entry(1)]
        ok, evidence = oracle.agrees([1, 2], shifted, [1, 2], shifted)
        self.assertFalse(ok)
        self.assertIn("do not align", evidence["failures"][0]["reason"])

    def test_prefix_length_mismatch_is_a_verdict(self):
        # One answer ends where the other continues: compare the shared positions, then
        # report the length difference instead of indexing past the shorter answer.
        ok, evidence = oracle.agrees([1, 2], [entry(1), entry(2)], [1], [entry(1)])
        self.assertFalse(ok)
        self.assertEqual(evidence["compared"], 1)
        self.assertIn("length mismatch at 1", evidence["failures"][-1]["reason"])
        ok, evidence = oracle.agrees([1], [entry(1)], [1, 2], [entry(1), entry(2)])
        self.assertFalse(ok)
        self.assertEqual(evidence["compared"], 1)
        self.assertIn("length mismatch at 1", evidence["failures"][-1]["reason"])

    def test_top5_swap_records_the_boundary_logprobs(self):
        base = alternatives()
        swapped = base[:4] + [base[5], base[4]] + base[6:]
        ok, evidence = oracle.agrees([1], [entry(1, base)], [1], [entry(1, swapped)])
        self.assertFalse(ok)
        failure = evidence["failures"][0]
        self.assertEqual(failure["reason"], "top-5 token sets differ")
        self.assertEqual([item["token"] for item in failure["cold_rank5_6"]], [4, 5])
        self.assertEqual([item["token"] for item in failure["measured_rank5_6"]], [5, 4])
        self.assertAlmostEqual(failure["cold_rank5_6"][0]["logprob"], base[4][1])

    def test_fewer_than_twenty_alternatives_fails(self):
        ok, evidence = oracle.agrees([1], [entry(1, alternatives(5))], [1],
                                     [entry(1, alternatives(5))])
        self.assertFalse(ok)
        self.assertIn("malformed or incomplete", evidence["failures"][0]["reason"])

    def test_duplicate_alternatives_fail(self):
        top = alternatives()
        duplicated = top[:-1] + [top[0]]
        ok, evidence = oracle.agrees([1], [entry(1, duplicated)], [1],
                                     [entry(1, duplicated)])
        self.assertFalse(ok)
        self.assertIn("malformed or incomplete", evidence["failures"][0]["reason"])

    def test_nan_logprob_fails(self):
        top = alternatives()
        top[3] = (top[3][0], math.nan)
        ok, evidence = oracle.agrees([1], [entry(1, top)], [1], [entry(1, top)])
        self.assertFalse(ok)
        self.assertIn("malformed or incomplete", evidence["failures"][0]["reason"])

    def test_infinite_sampled_logprob_fails(self):
        ok, evidence = oracle.agrees([1], [entry(1, own=math.inf)], [1],
                                     [entry(1, own=math.inf)])
        self.assertFalse(ok)
        self.assertIn("malformed or incomplete", evidence["failures"][0]["reason"])

    def test_zero_comparisons_fails(self):
        top = [(index, -6.0 - index) for index in range(oracle.TOP_LOGPROBS)]
        ok, evidence = oracle.agrees([1], [entry(1, top)], [1], [entry(1, top)])
        self.assertFalse(ok)
        self.assertIn("mass floor", evidence["failures"][0]["reason"])

    def test_empty_answer_fails(self):
        ok, evidence = oracle.agrees([], [], [], [])
        self.assertFalse(ok)
        self.assertEqual(evidence["failures"][0]["reason"], "empty answer")

    def noise_case(self):
        """Three positions with per-position deltas 0.0, 0.2, 0.6 and one top-5 swap."""
        cold = [entry(1), entry(2), entry(3)]
        swapped = alternatives()[:4] + [alternatives()[5], alternatives()[4]] + alternatives()[6:]
        measured = [entry(1), entry(2, jitter=0.2), entry(3, swapped, jitter=0.6)]
        return cold, measured

    def test_envelope_statistics_are_recorded(self):
        cold, measured = self.noise_case()
        ok, evidence = oracle.agrees([1, 2, 3], cold, [1, 2, 3], measured)
        self.assertFalse(ok, "the strict rule still rejects this")
        self.assertAlmostEqual(evidence["max_delta"], 0.6)
        self.assertAlmostEqual(evidence["mean_delta"], (0.0 + 0.2 + 0.6) / 3)
        self.assertAlmostEqual(evidence["top5_fraction"], 1 / 3)
        self.assertEqual(len(evidence["deltas"]), 3)

    def test_envelope_accepts_within_calibrated_bounds(self):
        cold, measured = self.noise_case()
        limits = oracle.envelope(mean_delta=0.3, max_delta=0.7, top5_fraction=0.5,
                                 absolute_max=2.0)
        ok, evidence = oracle.agrees([1, 2, 3], cold, [1, 2, 3], measured, limits=limits)
        self.assertTrue(ok, evidence["failures"])

    def test_envelope_rejects_beyond_mean(self):
        cold, measured = self.noise_case()
        limits = oracle.envelope(mean_delta=0.1, max_delta=0.7, top5_fraction=0.5)
        ok, evidence = oracle.agrees([1, 2, 3], cold, [1, 2, 3], measured, limits=limits)
        self.assertFalse(ok)
        self.assertIn("mean_delta", evidence["failures"][0]["reason"])

    def test_envelope_absolute_guard_rejects(self):
        cold, measured = self.noise_case()
        limits = oracle.envelope(mean_delta=1.0, max_delta=1.0, top5_fraction=1.0,
                                 absolute_max=0.5)
        ok, evidence = oracle.agrees([1, 2, 3], cold, [1, 2, 3], measured, limits=limits)
        self.assertFalse(ok)
        self.assertIn("absolute", evidence["failures"][0]["reason"])


class GateTests(unittest.TestCase):
    def test_inside_self_calibrated_limits(self):
        ok, gate = oracle.gate(mean=0.5, maximum=3.0, floor_mean=0.4, floor_max=2.5)
        self.assertTrue(ok, gate)
        self.assertFalse(gate["fallback_used"])
        self.assertAlmostEqual(gate["mean_limit"], 0.6)
        self.assertAlmostEqual(gate["max_limit"], oracle.ABSOLUTE_MAX)

    def test_mean_beyond_the_floor(self):
        ok, gate = oracle.gate(mean=0.7, maximum=3.0, floor_mean=0.4, floor_max=2.5)
        self.assertFalse(ok)
        self.assertIn("mean_delta", gate["failures"][0])

    def test_max_is_recorded_but_not_gated(self):
        # A maximum far above the floor still passes below the absolute guard.
        ok, gate = oracle.gate(mean=0.4, maximum=6.0, floor_mean=0.4, floor_max=2.5)
        self.assertTrue(ok, gate)
        self.assertAlmostEqual(gate["max_delta"], 6.0)

    def test_absolute_guard(self):
        ok, gate = oracle.gate(mean=0.4, maximum=12.0, floor_mean=0.4, floor_max=9.0)
        self.assertFalse(ok)
        self.assertTrue(any("absolute" in failure for failure in gate["failures"]))

    def test_fixed_fallback_without_a_floor(self):
        ok, gate = oracle.gate(mean=0.5, maximum=3.0, floor_mean=None, floor_max=None)
        self.assertTrue(ok, gate)
        self.assertTrue(gate["fallback_used"])
        self.assertAlmostEqual(gate["mean_limit"], oracle.FALLBACK_MEAN)

    def test_missing_statistics_fail(self):
        ok, gate = oracle.gate(mean=None, maximum=None, floor_mean=0.4, floor_max=2.5)
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main(verbosity=2)

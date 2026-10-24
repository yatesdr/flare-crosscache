"""Answer oracles for L2/L3: same-seed cold recomputes and distribution comparison.

Sampling is not greedy: temperature 0 is clamped into the package's declared range and
top_k is fixed by the package, so a tiny numeric shift can move the sampling CDF boundary
past the seed's draw and flip a token. A fixed seed therefore makes the *distribution*
reproducible, not the sampled token. Cache correctness is the distribution question.

A correctness request enables generation logprobs (`logprobs: true`, `top_logprobs: 20`,
`return_tokens_as_token_ids: true`) and `ignore_eos: true`, so end-of-sequence is an
ordinary token with a recorded distribution and every answer runs to its token limit. The
cold and the measured side request the same things; positions whose contexts are identical
(up to and including the first divergence) are compared token set and logprob by logprob:

- the top-5 token sets must be equal, and
- for every token in both top-20 lists with logprob at least -5, the logprob may differ by
  at most DISTRIBUTION_TOLERANCE nats.

Timed requests never enable logprobs, so their latency is unaffected.
"""

from __future__ import annotations

import http.client
import json
import math
import time
from urllib.parse import urlsplit

TOP_LOGPROBS = 20
TOP_SET = 5
COMPARED_MASS = -5.0
DISTRIBUTION_TOLERANCE = 0.05
ABSOLUTE_MAX = 10.0
FALLBACK_MEAN = 0.66
FALLBACK_MAX = 4.8


def correctness_body(body: dict) -> dict:
    """The same completion body as a correctness request with per-token distributions."""
    return {**body, "logprobs": True, "top_logprobs": TOP_LOGPROBS,
            "return_tokens_as_token_ids": True, "ignore_eos": True}


def stream(base_url: str, request: dict, timeout: float) -> dict:
    """One streaming completion returning token ids and their logprob entries.

    Mirrors Flare's `qualify.host_cache_isolation.stream` so callers can swap it in, and
    additionally collects `choices[0].logprobs.content`, aligned with `tokens`.
    """
    url = urlsplit(base_url)
    wants_logprobs = bool(request.get("logprobs"))
    request = {**request, "stream": True, "return_token_ids": True,
               "return_token_timestamps": True,
               "stream_options": {"include_usage": True}}
    payload = json.dumps(request).encode()
    conn = http.client.HTTPConnection(url.hostname, url.port, timeout=timeout)
    started = time.perf_counter_ns()
    tokens, logprobs, core, metadata, usage, reused = [], [], [], {}, {}, None
    errors, deliveries, content = [], [], []
    status = None
    done = False
    finish_reason = None
    try:
        conn.request("POST", "/v1/chat/completions", body=payload,
                     headers={"Content-Type": "application/json",
                              "Accept": "text/event-stream"})
        response = conn.getresponse()
        status = response.status
        if response.status != 200:
            errors.append(f"HTTP {response.status}: {response.read(1 << 16)[:200]!r}")
        data = []
        while response.status == 200 and (line := response.readline(1 << 20)):
            stripped = line.rstrip(b"\r\n")
            if stripped:
                if stripped.startswith(b"data:"):
                    data.append(stripped[5:].removeprefix(b" "))
                continue
            if not data:
                continue
            value = b"\n".join(data).decode()
            data.clear()
            if value == "[DONE]":
                done = True
                break
            event = json.loads(value)
            if "error" in event:
                errors.append(str(event["error"]))
            if event.get("performance") is not None:
                metadata = event["performance"]
            if event.get("usage") is not None:
                usage = event["usage"]
            if event.get("reused_prompt_tokens") is not None:
                reused = event["reused_prompt_tokens"]
            for choice in event.get("choices", []):
                delta = choice.get("delta") or {}
                if delta.get("content"):
                    content.append(delta["content"])
                if choice.get("finish_reason") is not None:
                    finish_reason = choice["finish_reason"]
                ids = choice.get("token_ids") or []
                stamps = choice.get("token_timestamps_ns") or []
                if ids:
                    tokens.extend(ids)
                    deliveries.append((time.perf_counter_ns() - started, len(ids)))
                    core.extend(stamps)
                entries = (choice.get("logprobs") or {}).get("content") or []
                logprobs.extend(entries)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        errors.append(f"{type(error).__name__}: {error}")
    finally:
        conn.close()
    if status == 200:
        if not done:
            errors.append("stream ended without [DONE]")
        if finish_reason is None:
            errors.append("stream ended without a finish reason")
        if wants_logprobs and len(logprobs) != len(tokens):
            errors.append(f"{len(logprobs)} logprob entries for {len(tokens)} generated tokens")
    return {"tokens": tokens, "logprobs": logprobs, "content": "".join(content),
            "core_ns": core, "deliveries": deliveries, "performance": metadata,
            "usage": usage, "reused": reused, "errors": errors,
            "finish_reason": finish_reason, "elapsed_ns": time.perf_counter_ns() - started}


def token_id(entry: dict) -> int | None:
    """The token id of a logprob entry, from its `token_id:` text or an explicit field."""
    token = entry.get("token") or ""
    prefix = "token_id:"
    if token.startswith(prefix):
        try:
            return int(token[len(prefix):])
        except ValueError:
            return None
    return entry.get("token_id")


def alternatives(entry: dict) -> tuple[dict[int, float], list[int]] | None:
    """The top logprobs of one position, or None unless exactly TOP_LOGPROBS unique,
    finite alternatives are present.

    Positions are compared in stream order, so an entry with a repeated or missing
    alternative is malformed rather than silently shorter: a shorter list would leave the
    gap it hides unexamined.
    """
    if not isinstance(entry, dict):
        return None
    top = entry.get("top_logprobs")
    if not isinstance(top, list) or len(top) != TOP_LOGPROBS:
        return None
    own = entry.get("logprob")
    if not isinstance(own, (int, float)) or not math.isfinite(own):
        return None
    values: dict[int, float] = {}
    order: list[int] = []
    for alternative in top:
        identifier = token_id(alternative)
        logprob = alternative.get("logprob")
        if identifier is None or identifier in values:
            return None
        if not isinstance(logprob, (int, float)) or not math.isfinite(logprob):
            return None
        values[identifier] = float(logprob)
        order.append(identifier)
    return values, order


def boundary(values: dict[int, float], order: list[int]) -> list[dict]:
    """The rank-5 and rank-6 entries, recorded so a top-5 swap can be told from a defect."""
    return [{"token": identifier, "logprob": values[identifier]}
            for identifier in order[TOP_SET - 1:TOP_SET + 1]]


def gate(mean, maximum, floor_mean, floor_max, absolute_max=ABSOLUTE_MAX,
         fallback_mean=FALLBACK_MEAN, fallback_max=FALLBACK_MAX):
    """The self-calibrating row gate.

    A row passes when its mean per-position delta stays inside max(1.5 x floor mean,
    floor mean + 0.15), derived from the same run's cold-vs-cold floor for that
    topology/tier, with an absolute 10-nat guard on any single difference. The maximum,
    top-5 fraction and exact tokens are recorded but not gated: a per-row maximum over a
    few dozen positions against a three-seed floor is an extreme-value statistic and does
    not discriminate. The fixed fallback mean applies only when no floor was measured,
    and the result says so. Missing statistics never pass.
    """
    if floor_mean is None:
        mean_limit = fallback_mean
        used_fallback = True
    else:
        mean_limit = max(1.5 * floor_mean, floor_mean + 0.15)
        used_fallback = False
    failures = []
    if mean is None or maximum is None:
        failures.append("no distribution statistics")
    else:
        if mean > mean_limit:
            failures.append(f"mean_delta {mean:.6f} exceeds {mean_limit:.6f}")
        if maximum > absolute_max:
            failures.append(f"max_delta {maximum:.6f} exceeds the absolute guard "
                            f"{absolute_max}")
    return not failures, {"passed": not failures, "failures": failures,
                          "mean_limit": mean_limit, "max_limit": absolute_max,
                          "absolute_max": absolute_max,
                          "floor_mean": floor_mean, "floor_max": floor_max,
                          "max_delta": maximum,
                          "floor_used": not used_fallback,
                          "fallback_used": used_fallback}


def envelope(mean_delta=None, max_delta=None, top5_fraction=None, absolute_max=None):
    """The calibrated comparison envelope.

    `mean_delta` and `max_delta` bound the per-position maximum shared-token logprob
    difference, `top5_fraction` bounds the share of compared positions whose top-5 sets
    differ, and `absolute_max` is a corruption guard on the largest single difference. Any
    field left None is not constrained. Passing limits at all selects envelope mode; without
    them the strict per-position rule (every delta within DISTRIBUTION_TOLERANCE and every
    top-5 set equal) applies.
    """
    return {"mean_delta": mean_delta, "max_delta": max_delta,
            "top5_fraction": top5_fraction, "absolute_max": absolute_max}


def compare_position(cold: dict | None, measured: dict | None) -> dict:
    """Measure one position's top-20 lists.

    Returns a hard `problem` when the position cannot be compared at all, otherwise its
    per-position delta (the largest shared-token logprob difference), whether the top-5
    sets match, and the boundary evidence for a mismatch.
    """
    if cold is None or measured is None:
        return {"problem": "missing distribution", "delta": None}
    parsed_cold, parsed_measured = alternatives(cold), alternatives(measured)
    if parsed_cold is None or parsed_measured is None:
        return {"problem": "malformed or incomplete alternatives", "delta": None}
    cold_values, cold_order = parsed_cold
    measured_values, measured_order = parsed_measured
    shared = [(abs(cold_values[identifier] - measured_values[identifier]), identifier)
              for identifier in cold_values
              if identifier in measured_values
              and cold_values[identifier] >= COMPARED_MASS
              and measured_values[identifier] >= COMPARED_MASS]
    if not shared:
        return {"problem": "no shared token above the mass floor", "delta": None}
    result = {"problem": None, "delta": max(shared)[0], "compared": len(shared),
              "top5_equal": set(cold_order[:TOP_SET]) == set(measured_order[:TOP_SET])}
    if not result["top5_equal"]:
        result["top5"] = {
            "cold_top5": cold_order[:TOP_SET], "measured_top5": measured_order[:TOP_SET],
            "cold_rank5_6": boundary(cold_values, cold_order),
            "measured_rank5_6": boundary(measured_values, measured_order),
        }
    return result


def aligned(tokens: list[int], entries: list[dict]) -> bool:
    """True when every entry carries the token id of the position it occupies."""
    if len(entries) != len(tokens):
        return False
    return all(token_id(entry) == token for token, entry in zip(tokens, entries))


def agrees(cold_tokens, cold_entries, measured_tokens, measured_entries, limits=None):
    """Distribution agreement over the positions whose contexts are identical.

    Positions are compared up to and including the first divergence; when the tokens are
    identical everywhere, every position is compared. The entries must line up one-to-one
    with their tokens: a missing or shifted entry would hide the position it belongs to, so
    that is a failure rather than a skip. Evidence always records the compared positions,
    the per-position deltas, mean and maximum delta, and the top-5 difference fraction.
    Without `limits` the strict per-position rule decides; with `limits` (see `envelope`)
    the calibrated envelope decides, with the absolute guard always applied.
    """
    cold_tokens, measured_tokens = list(cold_tokens), list(measured_tokens)
    cold_entries, measured_entries = list(cold_entries), list(measured_entries)
    minimum = min(len(cold_tokens), len(measured_tokens))
    prefix = (len(cold_tokens) != len(measured_tokens)
              and cold_tokens[:minimum] == measured_tokens[:minimum])
    divergence = next((position for position, (left, right)
                       in enumerate(zip(cold_tokens, measured_tokens)) if left != right),
                      minimum)
    if cold_tokens == measured_tokens:
        last = len(cold_tokens) - 1
    elif prefix:
        # One answer ends where the other continues; only the shared positions have
        # identical contexts, so the length difference is reported instead of indexed past
        # the shorter answer.
        last = minimum - 1
    else:
        last = divergence
    evidence = {
        "exact": cold_tokens == measured_tokens,
        "divergence": None if cold_tokens == measured_tokens else divergence,
        "compared": 0,
        "max_delta": None,
        "mean_delta": None,
        "top5_fraction": None,
        "top5_mismatches": 0,
        "deltas": [],
        "top5_differences": [],
        "failures": [],
    }
    if not cold_tokens or not measured_tokens:
        evidence["failures"].append({"position": 0, "reason": "empty answer"})
        return False, evidence
    if len(cold_entries) != len(cold_tokens) or len(measured_entries) != len(measured_tokens):
        evidence["failures"].append({
            "position": 0,
            "reason": "logprob entries do not match the generated token count",
            "cold": [len(cold_tokens), len(cold_entries)],
            "measured": [len(measured_tokens), len(measured_entries)],
        })
        return False, evidence
    if not aligned(cold_tokens, cold_entries) or not aligned(measured_tokens, measured_entries):
        evidence["failures"].append({
            "position": 0,
            "reason": "logprob entries do not align with the generated tokens",
        })
        return False, evidence
    for position in range(last + 1):
        result = compare_position(cold_entries[position], measured_entries[position])
        evidence["compared"] += 1
        if result["problem"] is not None:
            evidence["failures"].append({"position": position,
                                         "reason": result["problem"], "delta": None})
            continue
        evidence["deltas"].append({"position": position, "delta": result["delta"]})
        if not result["top5_equal"]:
            evidence["top5_mismatches"] += 1
            evidence["top5_differences"].append({"position": position, **result["top5"]})
    if prefix:
        evidence["failures"].append({
            "position": minimum,
            "reason": f"length mismatch at {minimum}",
            "cold_length": len(cold_tokens),
            "measured_length": len(measured_tokens),
        })
    if evidence["compared"] == 0:
        evidence["failures"].append({"position": 0, "reason": "no positions compared"})
    if evidence["deltas"]:
        values = [item["delta"] for item in evidence["deltas"]]
        evidence["max_delta"] = max(values)
        evidence["mean_delta"] = sum(values) / len(values)
        evidence["top5_fraction"] = evidence["top5_mismatches"] / evidence["compared"]
    if limits is None:
        for item in evidence["deltas"]:
            if item["delta"] > DISTRIBUTION_TOLERANCE:
                evidence["failures"].append({
                    "position": item["position"], "reason": "delta above tolerance",
                    "delta": item["delta"]})
        for difference in evidence["top5_differences"]:
            evidence["failures"].append({"reason": "top-5 token sets differ", **difference})
    else:
        for name in ("mean_delta", "max_delta", "top5_fraction"):
            measured = evidence[name]
            limit = limits.get(name)
            if limit is not None and measured is not None and measured > limit:
                evidence["failures"].append({
                    "position": None,
                    "reason": f"{name} {measured:.6f} exceeds {limit}"})
        absolute = limits.get("absolute_max")
        if (absolute is not None and evidence["max_delta"] is not None
                and evidence["max_delta"] > absolute):
            evidence["failures"].append({
                "position": None,
                "reason": f"max_delta {evidence['max_delta']:.6f} exceeds the absolute "
                          f"guard {absolute}"})
    return not evidence["failures"], evidence

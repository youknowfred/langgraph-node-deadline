"""Measure the per-call overhead of the hot-path primitives.

A reliability primitive that sits in the path of every node has to prove it adds no
meaningful cost. Run it::

    python bench/overhead.py

The numbers are nanoseconds per call, against a ``time.monotonic()`` baseline — and
they are 5-6 orders of magnitude smaller than the LLM/tool calls (100s of ms to
seconds) these primitives wrap. This script lives under ``bench/`` (not the package),
so it has zero packaging impact.
"""
import platform
import time
import timeit

from langgraph_node_deadline import (
    Hourglass,
    clamp_to_node_deadline,
    get_node_deadline_remaining_secs,
    node_deadline_in,
    node_deadline_scope,
    protected,
)

_HG = Hourglass(1000, {"out": protected(100)})


def _scope_cycle():
    with node_deadline_in(30.0):
        pass


def _grant_cycle():
    with _HG.grant("work"):
        pass


def _bench(label, fn, number=500_000):
    ns = timeit.timeit(fn, number=number) / number * 1e9
    print(f"  {label:<42}{ns:>10.1f}")


def main():
    print(
        f"\nlanggraph-node-deadline overhead  |  {platform.python_implementation()} "
        f"{platform.python_version()}  |  {platform.machine()}\n"
    )
    print(f"  {'operation':<42}{'ns/op':>10}")
    print(f"  {'-' * 42}{'-' * 10}")

    _bench("time.monotonic() [baseline]", time.monotonic)
    _bench("get_remaining()  (no scope, fail-open)", get_node_deadline_remaining_secs)
    _bench("clamp(60)        (no scope, fail-open)", lambda: clamp_to_node_deadline(60.0))

    deadline = time.monotonic() + 3600.0
    with node_deadline_scope(deadline):
        _bench("get_remaining()  (in scope)", get_node_deadline_remaining_secs)
        _bench(
            "clamp(60, reserve=2) (in scope)",
            lambda: clamp_to_node_deadline(60.0, reserve_secs=2.0),
        )

    _bench("node_deadline_in(30) enter+exit", _scope_cycle, number=200_000)
    _bench("Hourglass.grant() enter+exit", _grant_cycle, number=100_000)
    print("\n  (An LLM/tool call is 100s of ms to seconds — 5-6 orders larger.)\n")


if __name__ == "__main__":
    main()

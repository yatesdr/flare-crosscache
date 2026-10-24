#!/usr/bin/env bash
# L1: exact page/row bytes through the real manager, including DRAM, NVMe recovery and streaming.
#
# The Flare test binary must be built with the `server` feature and its ignored GPU test kept:
#   cargo test --release --features server --lib --no-run --message-format=json
# then pass the resulting binary path here. The test starts the manager itself through
# FLARE_POOL_TEST_MANAGER and writes scratch data under FLARE_POOL_TEST_DIR.
set -euo pipefail

usage() {
    echo "usage: $0 --manager PATH --test-binary PATH --dir PATH [--test NAME] [--dry-run]" >&2
    exit 2
}

manager=""
test_binary=""
directory=""
test_name="real_manager_cuda_round_trips_and_recovery"
dry_run=0
while [ $# -gt 0 ]; do
    case "$1" in
        --manager) manager=$2; shift 2 ;;
        --test-binary) test_binary=$2; shift 2 ;;
        --dir) directory=$2; shift 2 ;;
        --test) test_name=$2; shift 2 ;;
        --dry-run) dry_run=1; shift ;;
        -h|--help) usage ;;
        *) echo "unknown argument: $1" >&2; usage ;;
    esac
done
[ -n "$manager" ] && [ -n "$test_binary" ] && [ -n "$directory" ] || usage
[ -x "$manager" ] || { echo "manager is not executable: $manager" >&2; exit 2; }
[ -x "$test_binary" ] || { echo "test binary is not executable: $test_binary" >&2; exit 2; }

mkdir -p "$directory"
command=(
    env
    "FLARE_POOL_TEST_MANAGER=$manager"
    "FLARE_POOL_TEST_DIR=$directory"
    "FLARE_RUN_CUDA_TESTS=1"
    "$test_binary"
    --ignored
    --nocapture
    --test-threads=1
    "$test_name"
)
printf '%q ' "${command[@]}"; echo
if [ "$dry_run" -eq 1 ]; then
    exit 0
fi
"${command[@]}"

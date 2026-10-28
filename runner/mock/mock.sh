#!/bin/bash
# Mock worker (no model calls). Scenarios exercise the pipeline deterministically.
# good    : adds a passing regression test            -> ready_for_review
# bad     : adds a failing test every round           -> repair rounds, then failed (same failure)
# fixable : round 0 adds a failing test, round 1 fixes -> ready_for_review after 1 repair
# slow    : writes a partial change then sleeps 15 min -> used for cancel / restart tests
# noop    : changes nothing                           -> failed (no change)
# ask     : reports needs_input without changes       -> needs_input
# scope   : touches web/src/swe-pilot-mock.ts AND src/example_app/swe_pilot_mock.py every round -> a role=frontend job fails (role scope)
# scope-fixable : as scope in round 0; round 1 removes the python file -> a role=frontend job is ready_for_review
# scope-ok: touches web/src/swe-pilot-mock.ts and docs/swe-pilot-mock.md only (in scope for frontend/shared paths)
# data    : fails unless the data snapshot is copied to /work/data and /work/data/raw is read-only; then as good
set -u
scenario="${SWE_MOCK:-good}"; round="${SWE_ROUND:-0}"
say() { printf '{"type":"assistant","message":{"content":[{"type":"text","text":"%s"}]}}\n' "$1"; }
report() { printf '{"status":"%s","summary":"%s","checks_run":[],"limitations":["mock worker"],"questions":[%s]}\n' "$1" "$2" "${3:-}" > "$SWE_REPORT"; }
mkdir -p tests
T=tests/test_swe_pilot_mock.py
echo "mock: scenario=$scenario round=$round" >> "$SWE_CHECKPOINT"
say "mock worker scenario=$scenario round=$round"
case "$scenario" in
  good)
    printf 'def test_swe_pilot_mock_regression():\n    assert sum(range(5)) == 10\n' > $T
    report done "added a passing mock regression test" ;;
  bad)
    printf 'def test_swe_pilot_mock_regression():\n    assert sum(range(5)) == 11, "mock failure"\n' > $T
    report done "added a mock test (deliberately wrong)" ;;
  fixable)
    if [ "$round" = "0" ]; then
      printf 'def test_swe_pilot_mock_regression():\n    assert sum(range(5)) == 11, "mock failure"\n' > $T
      report done "first attempt (deliberately wrong)"
    else
      grep -q "== 11" $T || { echo "expected previous round edit to be present" >&2; exit 3; }
      sed -i 's/== 11, "mock failure"/== 10/' $T
      report done "fixed the assertion after reading validation evidence"
    fi ;;
  slow)
    printf '# partial work in progress\n' > tests/swe_pilot_partial.txt
    echo "mock: partial change written, sleeping" >> "$SWE_CHECKPOINT"
    for i in $(seq 1 900); do sleep 1; done
    report done "slow finished" ;;
  data)
    [ -s data/example.duckdb ] || { echo "mock data: /work/data/example.duckdb missing" >&2; exit 4; }
    [ -d data/processed ] || { echo "mock data: /work/data/processed missing" >&2; exit 4; }
    [ -d data/raw ] || { echo "mock data: /work/data/raw missing" >&2; exit 4; }
    if ( : > data/raw/.swe-ro-probe ) 2>/dev/null; then rm -f data/raw/.swe-ro-probe; echo "mock data: data/raw is writable" >&2; exit 5; fi
    echo "mock: data snapshot present, data/raw read-only" >> "$SWE_CHECKPOINT"
    printf 'def test_swe_pilot_mock_regression():\n    assert sum(range(5)) == 10\n' > $T
    report done "data snapshot and read-only mount verified; added a passing mock regression test" ;;
  scope|scope-fixable|scope-ok)
    mkdir -p web/src docs
    printf 'export const swePilotMock = 1\n' > web/src/swe-pilot-mock.ts
    if [ "$scenario" = scope-ok ]; then
      printf '# swe-pilot mock\n' > docs/swe-pilot-mock.md
      report done "touched only web/src and docs"
    elif [ "$scenario" = scope-fixable ] && [ "$round" != "0" ]; then
      rm -f src/example_app/swe_pilot_mock.py
      report done "reverted the out-of-scope python file"
    else
      mkdir -p src/example_app
      printf 'SWE_PILOT_MOCK = 1\n' > src/example_app/swe_pilot_mock.py
      report done "touched web/src and src/example_app"
    fi ;;
  noop) report done "did nothing" ;;
  ask) report needs_input "requirements unclear" '"Which endpoint should change?"' ;;
esac
printf '{"type":"result","subtype":"mock","is_error":false,"result":"mock %s done"}\n' "$scenario"

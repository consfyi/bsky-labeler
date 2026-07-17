#!/usr/bin/env bash
#
# Self-guard for .github/workflows/monitor.yml (the ops-alerting monitor).
#
# Dependency-free: bash + python3 (PyYAML), both present on ubuntu-latest.
# It (a) parses the workflow and `bash -n`-checks every step's run: block, and
# (b) behaviourally runs the alert/recovery blocks under the same shell GitHub
# uses (`bash --noprofile --norc -eo pipefail`) with stubbed curl/gh/date to
# assert the design invariant: the test step fails loudly when unconfigured,
# while the failure/recovery steps ALWAYS exit 0 so alerting problems never
# mask the monitor's own red/green.
#
# The run: blocks are extracted from the YAML (never duplicated here) so this
# test stays in sync with the real workflow.

set -euo pipefail

# PyYAML ships on ubuntu-latest today but is an implicit, unpinned dep; self-heal
# rather than red-X an unrelated PR if a future runner image drops it.
python3 -c "import yaml" 2>/dev/null || pip3 install --quiet pyyaml

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORKFLOW="$REPO_ROOT/.github/workflows/monitor.yml"

TMP="$(mktemp -d)"
# The workflow hard-codes these /tmp paths; clean up any we touch.
trap 'rm -rf "$TMP" /tmp/health_body /tmp/tg_resp' EXIT

FAILURES=0
pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; FAILURES=$((FAILURES + 1)); }

# Extract one step's run: block from the workflow by step name.
extract() {
  python3 - "$WORKFLOW" "$1" <<'PY'
import sys, yaml
wf = yaml.safe_load(open(sys.argv[1]))
name = sys.argv[2]
for step in wf["jobs"]["health"]["steps"]:
    if step.get("name") == name:
        sys.stdout.write(step["run"])
        sys.exit(0)
sys.stderr.write("step not found: %s\n" % name)
sys.exit(2)
PY
}

STEPS=(
  "Send test alert"
  "Probe labeler /health"
  "Alert ops channel (failure)"
  "Recovery notice"
)

# ---- (a) every run: block must parse -----------------------------------------
for name in "${STEPS[@]}"; do
  if ! extract "$name" >"$TMP/block.sh" 2>"$TMP/err"; then
    fail "extract run block: $name ($(cat "$TMP/err"))"
    continue
  fi
  if bash -n "$TMP/block.sh" 2>"$TMP/err"; then
    pass "bash -n parses: $name"
  else
    fail "bash -n: $name ($(cat "$TMP/err"))"
  fi
done

# ---- stubs -------------------------------------------------------------------
STUB="$TMP/stub"
mkdir -p "$STUB"

# curl stub: records args (for send-assertions), writes the -o body file, prints
# %{http_code} (STUB_CURL_CODE), and exits STUB_CURL_EXIT (transport failure).
cat >"$STUB/curl" <<'SH'
#!/usr/bin/env bash
[ -n "${STUB_ARGS:-}" ] && printf '%s\n' "$*" >>"$STUB_ARGS"
out=""; prev=""
for a in "$@"; do [ "$prev" = "-o" ] && out="$a"; prev="$a"; done
[ -n "$out" ] && printf '%s' "${STUB_CURL_BODY:-}" >"$out"
printf '%s' "${STUB_CURL_CODE:-200}"
exit "${STUB_CURL_EXIT:-0}"
SH

# gh stub: records args (for query-assertions), prints STUB_GH_CONCLUSION as
# the prior run's conclusion, or fails.
cat >"$STUB/gh" <<'SH'
#!/usr/bin/env bash
[ -n "${STUB_GH_ARGS:-}" ] && printf '%s\n' "$*" >>"$STUB_GH_ARGS"
[ "${STUB_GH_EXIT:-0}" != "0" ] && exit "$STUB_GH_EXIT"
printf '%s\n' "${STUB_GH_CONCLUSION:-success}"
SH

# date stub: deterministic timestamp.
cat >"$STUB/date" <<'SH'
#!/usr/bin/env bash
printf '2026-01-01T00:00Z'
SH

chmod +x "$STUB/curl" "$STUB/gh" "$STUB/date"

# Run a step's run: block under GitHub's default shell with stubs on PATH.
# Callers export OPS_TOKEN/OPS_CHAT/RUN_URL/... and STUB_* in a subshell first.
runblock() {
  # Guard the extraction itself: a failed or empty extract would truncate
  # run.sh to nothing, and an empty script exits 0 — making every "must exit 0"
  # assertion pass vacuously. return 97 is a non-zero the assertions treat as
  # failure, so a stale step name (behavioral literal drifting from the YAML)
  # is caught instead of silently going green.
  extract "$1" >"$TMP/run.sh" || { echo "EXTRACT-FAILED: $1" >&2; return 97; }
  [ -s "$TMP/run.sh" ] || { echo "EXTRACT-EMPTY: $1" >&2; return 97; }
  PATH="$STUB:$PATH" bash --noprofile --norc -eo pipefail "$TMP/run.sh"
}

# ---- (b1) "Send test alert" — must FAIL loudly when unusable ------------------
rc=0
( export OPS_TOKEN="" OPS_CHAT="c" RUN_URL="u"; runblock "Send test alert" ) >/dev/null 2>&1 || rc=$?
[ "$rc" != 0 ] && pass "test alert: unset OPS_TOKEN -> exit $rc (non-zero)" \
  || fail "test alert: unset OPS_TOKEN should be non-zero (got $rc)"

rc=0
( export OPS_TOKEN="t" OPS_CHAT=""  RUN_URL="u"; runblock "Send test alert" ) >/dev/null 2>&1 || rc=$?
[ "$rc" != 0 ] && pass "test alert: unset OPS_CHAT -> exit $rc (non-zero)" \
  || fail "test alert: unset OPS_CHAT should be non-zero (got $rc)"

rc=0
( export OPS_TOKEN="t" OPS_CHAT="c" RUN_URL="u" STUB_CURL_CODE="500"; runblock "Send test alert" ) >/dev/null 2>&1 || rc=$?
[ "$rc" != 0 ] && pass "test alert: curl non-200 -> exit $rc (non-zero)" \
  || fail "test alert: curl non-200 should be non-zero (got $rc)"

rc=0
( export OPS_TOKEN="t" OPS_CHAT="c" RUN_URL="u" STUB_CURL_CODE="200"; runblock "Send test alert" ) >/dev/null 2>&1 || rc=$?
[ "$rc" = 0 ] && pass "test alert: curl 200 -> exit 0" \
  || fail "test alert: curl 200 should exit 0 (got $rc)"

# ---- (b2) "Alert ops channel (failure)" — must ALWAYS exit 0 ------------------
# Worst case for errexit: no /tmp/health_body written by the (skipped) probe.
rm -f /tmp/health_body
rc=0
( export OPS_TOKEN="" OPS_CHAT=""; runblock "Alert ops channel (failure)" ) >/dev/null 2>&1 || rc=$?
[ "$rc" = 0 ] && pass "failure alert: secrets unset -> exit 0" \
  || fail "failure alert: secrets unset should exit 0 (got $rc)"

rc=0
( export OPS_TOKEN="t" OPS_CHAT="c" RUN_URL="u" STUB_CURL_CODE="500"; runblock "Alert ops channel (failure)" ) >/dev/null 2>&1 || rc=$?
[ "$rc" = 0 ] && pass "failure alert: Telegram 500 -> exit 0" \
  || fail "failure alert: Telegram 500 should exit 0 (got $rc)"

rc=0
( export OPS_TOKEN="t" OPS_CHAT="c" RUN_URL="u" STUB_CURL_EXIT="7"; runblock "Alert ops channel (failure)" ) >/dev/null 2>&1 || rc=$?
[ "$rc" = 0 ] && pass "failure alert: curl transport-fail -> exit 0" \
  || fail "failure alert: curl transport-fail should exit 0 (got $rc)"

# ---- (b3) "Recovery notice" — must ALWAYS exit 0, send only on prior failure --
rc=0
( export OPS_TOKEN="" OPS_CHAT=""; runblock "Recovery notice" ) >/dev/null 2>&1 || rc=$?
[ "$rc" = 0 ] && pass "recovery: secrets unset -> exit 0" \
  || fail "recovery: secrets unset should exit 0 (got $rc)"

rc=0
( export OPS_TOKEN="t" OPS_CHAT="c" RUN_URL="u" GITHUB_REPOSITORY="o/r" GH_TOKEN="g" \
    STUB_GH_CONCLUSION="failure" STUB_CURL_CODE="500"; runblock "Recovery notice" ) >/dev/null 2>&1 || rc=$?
[ "$rc" = 0 ] && pass "recovery: prior failure + Telegram 500 -> exit 0" \
  || fail "recovery: prior failure + Telegram 500 should exit 0 (got $rc)"

rc=0
( export OPS_TOKEN="t" OPS_CHAT="c" RUN_URL="u" GITHUB_REPOSITORY="o/r" GH_TOKEN="g" \
    STUB_GH_EXIT="1"; runblock "Recovery notice" ) >/dev/null 2>&1 || rc=$?
[ "$rc" = 0 ] && pass "recovery: gh api fails -> exit 0" \
  || fail "recovery: gh api failure should exit 0 (got $rc)"

# prior run failed AND probe green -> it sends a "recovered" message.
args="$TMP/rec_send"; : >"$args"; rc=0
( export OPS_TOKEN="t" OPS_CHAT="c" RUN_URL="u" GITHUB_REPOSITORY="o/r" GH_TOKEN="g" \
    STUB_GH_CONCLUSION="failure" STUB_CURL_CODE="200" STUB_ARGS="$args"; runblock "Recovery notice" ) >/dev/null 2>&1 || rc=$?
[ "$rc" = 0 ] && pass "recovery: prior failure + green -> exit 0" \
  || fail "recovery: prior failure + green should exit 0 (got $rc)"
grep -q "recovered" "$args" && pass "recovery: prior failure sends a 'recovered' message" \
  || fail "recovery: prior failure should send a 'recovered' message"

# prior run succeeded -> it must NOT send.
args="$TMP/rec_nosend"; : >"$args"; rc=0
( export OPS_TOKEN="t" OPS_CHAT="c" RUN_URL="u" GITHUB_REPOSITORY="o/r" GH_TOKEN="g" \
    STUB_GH_CONCLUSION="success" STUB_CURL_CODE="200" STUB_ARGS="$args"; runblock "Recovery notice" ) >/dev/null 2>&1 || rc=$?
[ "$rc" = 0 ] && pass "recovery: prior success -> exit 0" \
  || fail "recovery: prior success should exit 0 (got $rc)"
grep -q "recovered" "$args" && fail "recovery: prior success should NOT send" \
  || pass "recovery: prior success sends nothing"

# ---- (b4) lookback must filter to scheduled runs -----------------------------
# The lookback keys recovery on the previous run's conclusion, so manual
# workflow_dispatch runs (test_alert, force_fail) must be excluded: a green
# test_alert during an outage would otherwise show prev=success and suppress the
# real recovery notice, and a red dispatch would show prev=failure and fire a
# spurious "recovered". GitHub does that exclusion when the query carries
# event=schedule, so assert the gh api URL includes it.
ghargs="$TMP/rec_ghargs"; : >"$ghargs"; rc=0
( export OPS_TOKEN="t" OPS_CHAT="c" RUN_URL="u" GITHUB_REPOSITORY="o/r" GH_TOKEN="g" \
    STUB_GH_CONCLUSION="failure" STUB_CURL_CODE="200" STUB_GH_ARGS="$ghargs"; runblock "Recovery notice" ) >/dev/null 2>&1 || rc=$?
grep -q "event=schedule" "$ghargs" && pass "recovery: lookback query filters to event=schedule (dispatch runs excluded)" \
  || fail "recovery: gh api lookback must include event=schedule so test_alert/force_fail can't skew recovery"

echo
if [ "$FAILURES" -ne 0 ]; then
  echo "$FAILURES assertion(s) FAILED"
  exit 1
fi
echo "all monitor-workflow self-guard assertions passed"

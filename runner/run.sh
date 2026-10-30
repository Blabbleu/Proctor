#!/bin/bash
# proctor runner entrypoint. Runs INSIDE an isolated container as uid 10001.
# Modes: clone | checks | worker | export
set -uo pipefail
umask 002
export GIT_CONFIG_GLOBAL=/opt/proctor/gitconfig
export GIT_TERMINAL_PROMPT=0
mode="${1:?mode}"
OUT=/job/out
mkdir -p "$OUT" 2>/dev/null || true

json_str() { python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1" 2>/dev/null || printf '"%s"' "$(printf '%s' "$1" | tr -d '"\\\n')"; }

peak() { cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0; }

case "$mode" in
clone)
  # /mirror (ro) -> /work at $PROCTOR_COMMIT on branch $PROCTOR_BRANCH
  git clone -q --no-hardlinks --no-checkout /mirror /work/.clone-tmp || exit 20
  shopt -s dotglob; mv /work/.clone-tmp/* /work/ && rmdir /work/.clone-tmp || exit 21
  cd /work
  git -c core.hooksPath=/dev/null checkout -q -B "${PROCTOR_BRANCH:-proctor-work}" "$PROCTOR_COMMIT" || exit 22
  git remote remove origin
  test "$(git rev-parse HEAD)" = "$PROCTOR_COMMIT" || exit 23
  echo "{\"head\": \"$(git rev-parse HEAD)\"}" > "$OUT/clone.json"
  ;;

checks)
  # Fresh checkout of $PROCTOR_COMMIT, then prepare steps and registered checks from /job/in/checks.json.
  bash /opt/proctor/run.sh clone || { echo '{"error":"clone failed"}' > "$OUT/checks.json"; exit 30; }
  cd /work
  if [ -n "$(git status --porcelain)" ]; then echo '{"error":"checkout not clean"}' > "$OUT/checks.json"; exit 31; fi
  mkdir -p "$OUT/logs"
  # Runs with a data check ([[checks]] data = true): disposable data snapshot + read-only mounts, offline.
  if [ -n "${PROCTOR_DATA:-}" ]; then
    if [ -d /job/data ] && [ -n "$(ls -A /job/data 2>/dev/null)" ]; then
      cp -R --no-dereference --no-preserve=all /job/data/. /work/ || { echo '{"error":"data copy failed"}' > "$OUT/checks.json"; exit 32; }
    fi
    # [data].mount_ro entries arrive read-only at /job/ro/<n>; expose them at their project paths. (A bind mount
    # straight into the tmpfs checkout would leave a root-owned mount point that breaks the checkout above.)
    IFS='|' read -ra ro_map <<< "${PROCTOR_RO_MAP:-}"
    for ent in "${ro_map[@]}"; do
      [ -n "$ent" ] || continue
      rel="${ent%=*}"; idx="${ent##*=}"
      git ls-files -z -- "$rel" | xargs -0 -r git update-index --skip-worktree 2>/dev/null
      rm -rf "/work/$rel"; mkdir -p "$(dirname "/work/$rel")"
      ln -s "/job/ro/$idx" "/work/$rel" || { echo '{"error":"read-only mount link failed"}' > "$OUT/checks.json"; exit 33; }
    done
  fi
  python3 /opt/proctor/checks.py /job/in/checks.json "$OUT" "$PROCTOR_COMMIT"
  exit $?
  ;;

worker)
  cd /work
  start=$(date +%s)
  # Throwaway copies of project data ([data].copy), writable in the task checkout; copied once per job.
  if [ -d /job/data ] && [ -n "$(ls -A /job/data 2>/dev/null)" ] && [ ! -e /work/.proctor-data-copied ]; then
    cp -R --no-dereference --no-preserve=all /job/data/. /work/ \
      && : > /work/.proctor-data-copied \
      || { echo '{"exit": 42, "data_copy_failed": true}' > "$OUT/worker-exit.json"; exit 42; }
  fi
  if [ -f /job/in/prepare.sh ]; then
    bash /job/in/prepare.sh > "$OUT/prepare.log" 2>&1 || { echo '{"exit": 41, "prepare_failed": true}' > "$OUT/worker-exit.json"; exit 41; }
  fi
  case "$PROCTOR_HARNESS" in
    claude-code)
      args=(-p --output-format stream-json --verbose --model "$PROCTOR_MODEL"
            --max-turns "$PROCTOR_MAX_TURNS" --dangerously-skip-permissions --strict-mcp-config
            --disallowedTools WebFetch WebSearch)
      [ -n "${PROCTOR_MAX_BUDGET:-}" ] && args+=(--max-budget-usd "$PROCTOR_MAX_BUDGET")
      if [ -n "${PROCTOR_RESUME:-}" ]; then args+=(--resume "$PROCTOR_RESUME"); else args+=(--session-id "$PROCTOR_SESSION"); fi
      /opt/harness/claude "${args[@]}" < /job/in/prompt.md > "$OUT/worker.jsonl" 2> "$OUT/worker.stderr"
      rc=$?
      ;;
    codex)
      # Config lives in the per-job HOME; only the gateway is reachable, so point the custom provider there.
      mkdir -p "$CODEX_HOME"
      cat > "$CODEX_HOME/config.toml" <<EOF
model = "$PROCTOR_MODEL"
model_provider = "proctorgw"
model_reasoning_effort = "${PROCTOR_REASONING:-medium}"
approval_policy = "never"
sandbox_mode = "danger-full-access"
web_search = "disabled"
check_for_update_on_startup = false
project_doc_max_bytes = 65536

[analytics]
enabled = false

[feedback]
enabled = false

[history]
persistence = "none"

[shell_environment_policy]
inherit = "core"
exclude = ["PROCTOR_GATEWAY_TOKEN", "*TOKEN*", "*KEY*", "*SECRET*"]

[model_providers.proctorgw]
name = "proctor gateway"
base_url = "$PROCTOR_GATEWAY_URL"
env_key = "PROCTOR_GATEWAY_TOKEN"
wire_api = "responses"
request_max_retries = 2
stream_max_retries = 3
stream_idle_timeout_ms = 300000
supports_websockets = false
EOF
      cargs=(--json --skip-git-repo-check --dangerously-bypass-approvals-and-sandbox --cd /work
             -o "$OUT/last-message.txt")
      if [ -n "${PROCTOR_RESUME:-}" ]; then
        /opt/harness/codex exec resume "${cargs[@]}" "$PROCTOR_RESUME" - < /job/in/prompt.md > "$OUT/worker.jsonl" 2> "$OUT/worker.stderr"
      else
        /opt/harness/codex exec "${cargs[@]}" - < /job/in/prompt.md > "$OUT/worker.jsonl" 2> "$OUT/worker.stderr"
      fi
      rc=$?
      ;;
    mock)
      bash /opt/proctor/mock/mock.sh > "$OUT/worker.jsonl" 2> "$OUT/worker.stderr"
      rc=$?
      ;;
    *) echo "unknown harness $PROCTOR_HARNESS" > "$OUT/worker.stderr"; rc=40 ;;
  esac
  echo "{\"exit\": $rc, \"seconds\": $(( $(date +%s) - start )), \"memory_peak\": $(peak)}" > "$OUT/worker-exit.json"
  exit $rc
  ;;

export)
  # Untrusted workspace /w (ro) -> patch against $PROCTOR_BASE computed in a TRUSTED fresh clone.
  # The worker's .git (config, hooks, index, objects) is never used.
  clean="$OUT/clean"
  rm -rf "$clean"; mkdir -p "$clean"
  git clone -q --no-hardlinks --no-checkout /mirror "$clean" || exit 50
  cd "$clean" && git -c core.hooksPath=/dev/null checkout -q --detach "$PROCTOR_BASE" || exit 51
  excludes=(--exclude=/.git)
  while IFS= read -r e; do [ -n "$e" ] && excludes+=("--exclude=$e"); done < /job/in/export-exclude.txt
  rsync -a --delete --no-specials --no-devices "${excludes[@]}" /w/ "$clean/" || exit 52
  git -c core.hooksPath=/dev/null add -A || exit 53
  git diff --cached --binary --full-index "$PROCTOR_BASE" > "$OUT/candidate.patch" || exit 54
  git diff --cached --name-status "$PROCTOR_BASE" > "$OUT/files.txt"
  git diff --cached --stat "$PROCTOR_BASE" > "$OUT/stat.txt"
  cd / && rm -rf "$clean"
  ;;
*)
  echo "unknown mode $mode" >&2; exit 2 ;;
esac

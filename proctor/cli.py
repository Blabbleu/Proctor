"""`proctor` command-line interface: submit / status / list / logs / cancel / retry / usage / projects / project add / doctor.

Works for root and members of group `proctor`. Writes only to the job store; the supervisor does the work.
"""
from __future__ import annotations

import argparse
import json
import os
import pwd
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from proctor import db, harness  # noqa: E402
from proctor.common import (ACTIVE, ETC, JOB_ID_RE, PROJECT_ID_RE, STATE, TERMINAL, ProjectError, iso, list_projects,  # noqa: E402
                              load_project, load_settings, model_allowed, new_job_id, now, tail_text,
                              PROVIDER_LABEL, QUOTA_ROW, provider_for, role_model)
from proctor.broker import quota_block_reason, remaining_allowance  # noqa: E402


def out(obj, as_json, text):
    print(json.dumps(obj, indent=1, default=str) if as_json else text)


def need_job(conn, jid):
    if not JOB_ID_RE.match(jid or ""):
        sys.exit(f"invalid job id {jid!r}")
    j = db.job(conn, jid)
    if not j:
        sys.exit(f"no job {jid}")
    return j


def cmd_submit(a, conn):
    try:
        p = load_project(a.project)
    except ProjectError as e:
        sys.exit(str(e))
    if not p["enabled"]:
        sys.exit(f"project {p['id']} is disabled in the registry")
    settings = load_settings()
    role = getattr(a, "role", None) or None
    if role and role not in p["roles"]:
        sys.exit(f"unknown role {role!r} for {p['id']} (defined: {', '.join(sorted(p['roles'])) or 'none'})")
    if role and not a.model and a.harness != "mock":
        a.model = role_model(p, role, lambda prov: quota_block_reason(conn, settings, prov)) or a.model
    hname = a.harness
    if not hname and a.model:
        # A model implies its harness unless one was named: GPT models run in Codex, Claude in Claude Code.
        hname = harness.default_harness_for(a.model, p)
    hname = hname or p["harness"]["default"]
    if hname not in harness.ADAPTERS:
        sys.exit(f"unknown harness {hname}; available: {', '.join(harness.ADAPTERS)}")
    if hname != "mock" and hname not in p["harness"]["allowed"]:
        sys.exit(f"harness {hname} not allowed for {p['id']} (allowed: {p['harness']['allowed']})")
    binkey = {"claude-code": "claude_binary", "codex": "codex_binary"}.get(hname)
    if binkey and not settings["harness"][binkey]:
        sys.exit(f"harness {hname} is not installed: set [harness].{binkey} in settings.toml")
    model = a.model or (p["harness"].get("default_model") if hname != "mock" else "mock")
    if hname != "mock" and not a.model:
        model = (p["harness"].get("default_models") or {}).get(hname) or model
    if hname != "mock" and not model_allowed(p, settings, model):
        sys.exit(f"model {model!r} is not approved for {p['id']} or has no price entry")
    if hname != "mock" and harness.ADAPTERS[hname].provider != provider_for(model):
        sys.exit(f"model {model!r} cannot run in harness {hname} (it serves {harness.ADAPTERS[hname].provider} models)")
    if hname == "mock" and a.mock not in ("good", "bad", "slow", "noop", "ask", "fixable", "data", "scope", "scope-fixable", "scope-ok"):
        sys.exit("--mock must be good|bad|slow|noop|ask|fixable|data|scope|scope-fixable|scope-ok")
    acc = [x.strip() for x in (a.accept or []) if x.strip()]
    goal = (a.goal or "").strip()
    title = (a.title or goal.splitlines()[0] if goal else "").strip()[:120]
    state, reason = "queued", ""
    if len(goal) < 15 or not acc or sum(len(x) for x in acc) < 10:
        state, reason = "needs_input", "a goal (≥15 chars) and at least one concrete --accept criterion are required"
    if hname != "mock" and state == "queued":
        b = settings.get("budget", {})
        if not (b.get("per_task_usd", 0) > 0 and b.get("per_day_usd", 0) > 0 and b.get("per_month_usd", 0) > 0):
            sys.exit("no spending ceiling configured in /etc/proctor/settings.toml [budget]; refusing paid runs")
    key = a.key or None
    if key:
        ex = conn.execute("SELECT id, state FROM jobs WHERE idem_key=? AND project=?", (key, p["id"])).fetchone()
        if ex:
            out({"id": ex["id"], "state": ex["state"], "duplicate": True}, a.json,
                f"duplicate submission: key {key!r} is job {ex['id']} ({ex['state']})")
            return
    jid = new_job_id()
    meta = {"mock": a.mock} if hname == "mock" else {}
    if role:
        meta["role"] = role
    conn.execute("INSERT INTO jobs(id, project, title, goal, acceptance, harness, model, state, reason, idem_key, created_at,"
                 " notify, retry_of, meta, finished_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (jid, p["id"], title or "(untitled)", goal, json.dumps(acc), hname, model, state, reason, key, now(),
                  0 if a.quiet else 1, a.retry_of, json.dumps(meta), now() if state != "queued" else None))
    db.event(conn, jid, "submit", f"submitted by {os.environ.get('SUDO_USER') or os.environ.get('USER', '?')} via cli",
             harness=hname, model=model)
    out({"id": jid, "state": state, "reason": reason}, a.json,
        f"{jid} {state}" + (f": {reason}" if reason else f" ({p['id']}, {hname}/{model})"))


def fmt_cost(conn, jid):
    r = conn.execute("SELECT COUNT(*), COALESCE(SUM(cost_usd),0) FROM usage WHERE job_id=?", (jid,)).fetchone()
    return r[0], r[1]


def cmd_list(a, conn):
    q = "SELECT * FROM jobs"
    args = []
    if not a.all:
        q += " WHERE created_at > ?"
        args.append(now() - 7 * 86400)
    rows = conn.execute(q + " ORDER BY created_at DESC LIMIT ?", (*args, a.limit)).fetchall()
    if a.json:
        return out([dict(r) for r in rows], True, "")
    if not rows:
        print("no jobs")
    for r in rows:
        n, c = fmt_cost(conn, r["id"])
        print(f"{r['id']}  {r['state']:<19} {r['project']:<10} {r['harness']:<11} ${c:5.2f}  {iso(r['created_at'])}  {r['title'][:50]}")


def cmd_status(a, conn):
    j = need_job(conn, a.id)
    n, c = fmt_cost(conn, j["id"])
    m = db.meta(j)
    d = dict(j)
    d["meta"] = m
    d["usage"] = {"calls": n, "cost_usd_list_price": round(c, 4)}
    d["checks"] = [dict(r) for r in conn.execute("SELECT stage, name, status, exit_code, duration, commit_sha, log_path FROM checks WHERE job_id=? ORDER BY id", (j["id"],))]
    d["attempts"] = [dict(r) for r in conn.execute("SELECT id, kind, round, status, exit_code, started_at, ended_at FROM attempts WHERE job_id=? ORDER BY seq", (j["id"],))]
    if a.json:
        return out(d, True, "")
    art = STATE / "jobs" / j["id"] / "artifacts"
    lines = [f"{j['id']}  {j['state']}  ({j['step'] or '-'})  project={j['project']}" + (f"  role={m['role']}" if m.get("role") else ""),
             f"title:     {j['title']}",
             f"harness:   {j['harness']} / {j['model']}   round {j['round']}   attempts {j['attempt_seq']}   infra-retries {j['infra_retries']}",
             f"created:   {iso(j['created_at'])}   started {iso(j['started_at'])}   finished {iso(j['finished_at'])}   deadline {iso(j['deadline'])}",
             f"base:      {j['base_branch']} @ {j['base_commit'] or '-'}",
             f"candidate: {j['candidate_commit'] or '-'}",
             f"usage:     {n} model calls, ≈ ${c:.3f} at list price (gateway-metered estimate)"]
    if j["reason"]:
        lines.append(f"reason:    {j['reason']}")
    if j["publication"]:
        p = json.loads(j["publication"])
        lines.append(f"published: branch {p.get('branch')} in {p.get('repo')} (local={p.get('local')}, remote={p.get('remote')})")
        lines.append(f"patch:     {p.get('patch')}")
    if d["checks"]:
        lines.append("checks (supervisor-run, independent of the agent):")
        for r in d["checks"]:
            lines.append(f"  [{r['stage']:<12}] {r['name']:<12} {r['status']:<7} exit={r['exit_code']} {r['duration']:.1f}s @ {(r['commit_sha'] or '')[:10]}")
    rep = m.get("report") or {}
    if rep:
        lines.append(f"worker report (claims, not evidence): {rep.get('status')}: {str(rep.get('summary'))[:400]}")
    if art.exists():
        lines.append(f"artifacts: {art}")
    print("\n".join(lines))


def cmd_logs(a, conn):
    j = need_job(conn, a.id)
    rows = conn.execute("SELECT * FROM events WHERE job_id=? ORDER BY id", (j["id"],)).fetchall()
    for r in rows[-a.n:]:
        print(f"{iso(r['ts'])}  {r['kind']:<10} {r['msg']}")
    if a.worker:
        jd = STATE / "jobs" / j["id"] / "out"
        for f in sorted(jd.glob("worker-r*/worker.jsonl")):
            print(f"--- {f} (tail)")
            for line in tail_text(f, 6000).splitlines()[-15:]:
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                t = ev.get("type")
                if t == "assistant":
                    for b in (ev.get("message") or {}).get("content") or []:
                        if b.get("type") == "text":
                            print("  assistant:", b["text"][:300].replace("\n", " "))
                        elif b.get("type") == "tool_use":
                            print("  tool:", b.get("name"), json.dumps(b.get("input"))[:200])
                elif t == "result":
                    print("  result:", ev.get("subtype"), str(ev.get("result"))[:300])
    if a.check:
        for r in conn.execute("SELECT * FROM checks WHERE job_id=? AND name=? ORDER BY id DESC LIMIT 1", (j["id"], a.check)):
            print(f"--- {r['stage']} {r['name']} {r['status']} ({r['log_path']})")
            print(tail_text(Path(r["log_path"]), 8000))


def cmd_cancel(a, conn):
    j = need_job(conn, a.id)
    if j["state"] in TERMINAL:
        sys.exit(f"{j['id']} is already {j['state']}")
    conn.execute("UPDATE jobs SET cancel_requested=1 WHERE id=?", (j["id"],))
    db.event(conn, j["id"], "cancel", "cancel requested via cli")
    print(f"{j['id']} cancel requested (supervisor stops the runner within a few seconds; work in progress is preserved)")


def cmd_retry(a, conn):
    j = need_job(conn, a.id)
    if j["state"] not in TERMINAL or (j["state"] == "ready_for_review" and not getattr(a, "allow_ready", False)):
        sys.exit(f"{j['id']} is {j['state']}; only failed/blocked/cancelled/interrupted/needs_input jobs can be retried")
    a.project, a.goal, a.title = j["project"], a.goal or j["goal"], j["title"]
    a.accept = a.accept or json.loads(j["acceptance"])
    a.harness = a.harness or j["harness"]
    a.model = a.model or (j["model"] if j["harness"] != "mock" else None)
    a.mock = a.mock or db.meta(j).get("mock", "good")
    a.role = getattr(a, "role", None) or db.meta(j).get("role")
    a.key, a.retry_of, a.quiet = None, j["id"], a.quiet
    cmd_submit(a, conn)


def cmd_usage(a, conn):
    s = load_settings()
    b = s["budget"]
    import time as _t
    day = _t.strftime("%Y-%m-%d")
    d = conn.execute("SELECT COUNT(*), COALESCE(SUM(cost_usd),0) FROM usage WHERE day=?", (day,)).fetchone()
    mo = conn.execute("SELECT COUNT(*), COALESCE(SUM(cost_usd),0) FROM usage WHERE substr(day,1,7)=?", (day[:7],)).fetchone()
    blocks = conn.execute("SELECT * FROM gateway_blocks ORDER BY id DESC LIMIT 5").fetchall()
    quotas = {}
    for prov, row in QUOTA_ROW.items():
        q = conn.execute("SELECT * FROM quota WHERE id=?", (row,)).fetchone()
        quotas[prov] = {"reading": dict(q) if q else None, "block": quota_block_reason(conn, s, prov)}
    res = {"today": {"calls": d[0], "usd": d[1], "cap": b["per_day_usd"]},
           "month": {"calls": mo[0], "usd": mo[1], "cap": b["per_month_usd"]},
           "per_task_cap": b["per_task_usd"], "quota": quotas,
           "recent_refusals": [dict(r) for r in blocks]}
    if a.json:
        return out(res, True, "")
    pct = lambda v: "?" if v is None else f"{v:.0%}"  # noqa: E731
    print(f"Gateway-metered usage at list price (enforced caps: task ${b['per_task_usd']}, day ${b['per_day_usd']}, month ${b['per_month_usd']})")
    print(f"  today: {d[0]} calls, ${d[1]:.3f}    month: {mo[0]} calls, ${mo[1]:.3f}")
    g = s.get("quota", {})
    if not g.get("enabled", True):
        print("  quota guard: off ([quota].enabled = false)")
    else:   # same defaults as broker.quota_block_reason
        print(f"  quota guard (monitored threshold): pause model calls at 5h ≥ {g.get('max_5h', 0.85):.0%} or 7d ≥ {g.get('max_7d', 0.90):.0%}")
    for prov, v in quotas.items():
        q = v["reading"]
        seen = f"5h {pct(q['util_5h'])}, 7d {pct(q['util_7d'])} (seen {iso(q['ts'])} via {q['source']})" if q else "no reading yet"
        print(f"  {PROVIDER_LABEL[prov]:8s} {seen} -> {v['block'] or 'allowed'}")
    for r in blocks:
        print(f"  refused {iso(r['ts'])} {r['job_id']} {r['kind']}: {r['reason'][:120]}")


def cmd_projects(a, conn):
    res = []
    for pid in list_projects():
        try:
            p = load_project(pid)
            res.append({"id": pid, "name": p.get("name"), "base_branch": p["base_branch"], "harness": p["harness"]["default"],
                        "model": p["harness"].get("default_model"), "checks": [c["name"] for c in p["checks"]],
                        "publish": p["publish"], "enabled": p["enabled"]})
        except Exception as e:
            res.append({"id": pid, "error": str(e)})
    if a.json:
        return out(res, True, "")
    for r in res:
        if "error" in r:
            print(f"{r['id']}: INVALID: {r['error']}")
        else:
            print(f"{r['id']}: {r['name']} · base {r['base_branch']} · {r['harness']}/{r['model']} · checks {', '.join(r['checks'])} · "
                  f"push={'on' if r['publish']['remote_push'] else 'off'} pr={'on' if r['publish']['draft_pr'] else 'off'}"
                  + ("" if r["enabled"] else " · DISABLED"))


def cmd_doctor(a, conn):
    """Health report. BAD fails the exit code; WARN is something to fix before real (non-mock) work."""
    ok = True

    def chk(name, cond, detail="", warn=False, hint=""):
        nonlocal ok
        if not cond and not warn:
            ok = False
        detail = detail if cond or not hint else hint
        print(f"{'ok  ' if cond else ('WARN' if warn else 'BAD ')} {name}" + (f" — {detail}" if detail else ""))
    for svc in ("proctor-firewall", "proctor-broker", "proctor-supervisor"):
        r = subprocess.run(["systemctl", "is-active", svc], capture_output=True, text=True)
        chk(f"service {svc}", r.stdout.strip() == "active", r.stdout.strip())
    try:
        s = load_settings()
        chk("settings.toml", True)
    except Exception as e:
        chk("settings.toml", False, str(e))
        sys.exit(1)
    r = subprocess.run(["docker", "network", "inspect", s["gateway"]["network"]], capture_output=True, text=True)
    chk(f"docker network {s['gateway']['network']}", r.returncode == 0, (r.stderr.strip() or "")[:200])
    if os.geteuid() == 0:
        r = subprocess.run(["iptables", "-C", "INPUT", "-i", s["gateway"]["interface"], "-j", "PROCTOR-GW-IN"],
                           capture_output=True, text=True)
        chk("firewall chain PROCTOR-GW-IN", r.returncode == 0, hint="run: systemctl restart proctor-firewall")
    for key, prov in (("claude_binary", "anthropic"), ("codex_binary", "openai")):
        path = s["harness"][key]
        if path:
            chk(f"harness {key}", os.access(path, os.X_OK), path)
            src = s["credentials"][prov]
            if src == "env":
                var = "ANTHROPIC_API_KEY" if prov == "anthropic" else "OPENAI_API_KEY"
                chk(f"{prov} credential", _env_file_has(var), hint=f"set {var} in {ETC / 'credentials.env'}", warn=True)
            else:
                chk(f"{prov} credential", Path(s["hermes"]["agent_dir"]).is_dir(), f"hermes at {s['hermes']['agent_dir']}")
        else:
            chk(f"harness {key}", False, hint="not set; only the mock harness can run", warn=True)
    b = s["budget"]
    chk("spending ceiling configured", b.get("per_task_usd", 0) > 0 and b.get("per_day_usd", 0) > 0,
        f"task ${b.get('per_task_usd')} day ${b.get('per_day_usd')} month ${b.get('per_month_usd')}", warn=True)
    a_ = conn.execute(f"SELECT COUNT(*) FROM jobs WHERE state IN ({','.join('?' * len(ACTIVE))})", ACTIVE).fetchone()[0]
    chk("at most one active job", a_ <= 1, f"{a_} active")
    pids = list_projects()
    chk("projects registered", bool(pids), ", ".join(pids), hint="add one: proctor project add <id> --path /path/to/repo", warn=True)
    for pid in pids:
        try:
            p = load_project(pid)
            chk(f"project {pid}", Path(p["source"]["path"]).is_dir(), hint=f"source {p['source']['path']} missing")
        except Exception as e:
            chk(f"project {pid}", False, str(e))
    sys.exit(0 if ok else 1)


def _env_file_has(var):
    """Whether credentials.env sets var to a non-empty value (unreadable counts as unknown -> False)."""
    try:
        text = (ETC / "credentials.env").read_text()
    except OSError:
        return False
    return any(line.split("=", 1)[0].strip() == var and line.split("=", 1)[1].strip().strip("\"'")
               for line in text.splitlines() if "=" in line and not line.lstrip().startswith("#"))


def _git_head_branch(repo: Path) -> str | None:
    """Current branch from .git/HEAD, read as a file so root never runs git inside another user's repo."""
    try:
        head = (repo / ".git" / "HEAD").read_text().strip()
    except OSError:
        return None
    return head.removeprefix("ref: refs/heads/") if head.startswith("ref: refs/heads/") else None


def _toml_str(v: str) -> str:
    return json.dumps(v)   # a JSON string is a valid TOML basic string


def cmd_project_add(a, conn):
    if not PROJECT_ID_RE.match(a.id):
        sys.exit(f"invalid project id {a.id!r} (lowercase letters, digits, '-')")
    repo = Path(a.path).resolve()
    if not (repo / ".git").exists():
        sys.exit(f"{repo} is not a git repository")
    owner = a.owner or pwd.getpwuid(repo.stat().st_uid).pw_name
    if owner == "root":
        sys.exit(f"{repo} is owned by root; proctor needs a non-root repository owner (--owner)")
    branch = a.branch or _git_head_branch(repo)
    if not branch:
        sys.exit("could not read the current branch; pass --branch")
    checks = []
    for c in a.check or []:
        name, sep, command = c.partition("=")
        if not sep or not command.strip():
            sys.exit(f"--check must look like name=command, got {c!r}")
        checks.append((name.strip(), command.strip()))
    if a.harness != "mock" and not a.model:
        sys.exit(f"--harness {a.harness} needs --model (and a [prices] entry for it in settings.toml)")
    d = ETC / "projects" / a.id
    if (d / "project.toml").exists() and not a.force:
        sys.exit(f"project {a.id} already exists at {d} (use --force to overwrite project.toml)")
    allowed = sorted({a.harness, "mock"})
    lines = [f"id = {_toml_str(a.id)}", f"name = {_toml_str(a.name or a.id)}", "enabled = true",
             f"base_branch = {_toml_str(branch)}", "",
             "[source]", f"path = {_toml_str(str(repo))}", f"owner = {_toml_str(owner)}", "",
             "[harness]", f"default = {_toml_str(a.harness)}", f"allowed = {json.dumps(allowed)}",
             f"models = {json.dumps([a.model] if a.model else [])}"]
    if a.model:
        lines.append(f"default_model = {_toml_str(a.model)}")
    lines += ["", "[publish]", "# A reviewable proctor/<job> branch in the source repo; nothing is pushed.",
              "local_branch = true", "remote_push = false", "draft_pr = false", "",
              "[orchestrator]", "enabled = false", 'merge = "none"', "push = false"]
    for name, command in checks:
        lines += ["", "[[checks]]", f"name = {_toml_str(name)}", f"command = {_toml_str(command)}"]
    d.mkdir(parents=True, exist_ok=True)
    (d / "project.toml").write_text("\n".join(lines) + "\n")
    if not (d / "Dockerfile").exists():
        (d / "Dockerfile").write_text("ARG BASE\nFROM ${BASE}\n# Install the project's build/test dependencies here: checks and workers run offline.\n")
    if not (d / "orchestrator.md").exists():
        (d / "orchestrator.md").write_text(f"# {a.name or a.id}\n\nDescribe the project's goals and constraints here before enabling orchestration.\n")
    try:
        load_project(a.id)
    except ProjectError as e:
        sys.exit(f"wrote {d / 'project.toml'} but it does not validate: {e}")
    print(f"registered {a.id}: {repo} (owner {owner}, base {branch}, harness {a.harness})")
    if not checks:
        print("note: no checks registered; add [[checks]] (or re-run with --check name=command) so candidates are validated")
    print(f"dependencies: edit {d / 'Dockerfile'} — runners have no network at check time")
    print(f"try it:  proctor submit {a.id} 'Describe a small change' -a 'what done looks like' --harness mock")


def main():
    ap = argparse.ArgumentParser(prog="proctor", description="proctor: autonomous coding tasks with independent validation")
    ap.add_argument("--json", action="store_true")
    sp = ap.add_subparsers(dest="cmd", required=True)
    s = sp.add_parser("submit", help="queue a task")
    s.add_argument("project")
    s.add_argument("goal")
    s.add_argument("-a", "--accept", action="append", help="acceptance criterion (repeatable, required)")
    s.add_argument("-t", "--title")
    s.add_argument("--harness")
    s.add_argument("--model")
    s.add_argument("--mock", default="good", help="mock worker scenario (harness=mock only)")
    s.add_argument("--role", help="worker role from the project's [roles.*]: restricts the paths the change may touch")
    s.add_argument("--key", help="idempotency key (duplicate submissions return the existing job)")
    s.add_argument("--quiet", action="store_true", help="no Discord notification")
    s.set_defaults(retry_of=None)
    ls = sp.add_parser("list")
    ls.add_argument("--all", action="store_true")
    ls.add_argument("-n", "--limit", type=int, default=20)
    st = sp.add_parser("status")
    st.add_argument("id")
    lg = sp.add_parser("logs")
    lg.add_argument("id")
    lg.add_argument("-n", type=int, default=40)
    lg.add_argument("--worker", action="store_true", help="tail the worker transcript")
    lg.add_argument("--check", help="print a check log")
    c = sp.add_parser("cancel")
    c.add_argument("id")
    r = sp.add_parser("retry", help="resubmit a finished job as a new job (same task, fresh base)")
    r.add_argument("id")
    r.add_argument("--goal")
    r.add_argument("-a", "--accept", action="append")
    r.add_argument("--harness")
    r.add_argument("--model")
    r.add_argument("--mock")
    r.add_argument("--role")
    r.add_argument("--quiet", action="store_true")
    sp.add_parser("usage")
    sp.add_parser("projects")
    sp.add_parser("doctor")
    pj = sp.add_parser("project", help="register projects").add_subparsers(dest="project_cmd", required=True)
    pa = pj.add_parser("add", help="register a git repository as a project")
    pa.add_argument("id")
    pa.add_argument("--path", required=True, help="the source repository")
    pa.add_argument("--owner", help="non-root user that owns the repository (default: the directory's owner)")
    pa.add_argument("--branch", help="base branch (default: the repository's current branch)")
    pa.add_argument("--name")
    pa.add_argument("--check", action="append", metavar="NAME=COMMAND", help="validation check (repeatable)")
    pa.add_argument("--harness", default="mock", choices=sorted(harness.ADAPTERS))
    pa.add_argument("--model", help="model for a real harness (must also be priced in settings.toml)")
    pa.add_argument("--force", action="store_true", help="overwrite an existing project.toml")
    o = sp.add_parser("orch", help="orchestrator tools (gate/context/review/submit/merge/...); see `proctor orch -h`")
    from proctor import orch  # noqa: E402  (only the orch sub-commands depend on it)
    orch.add_parsers(o.add_subparsers(dest="orch_cmd", required=True, metavar="COMMAND"))
    a = ap.parse_args()
    os.umask(0o007)
    conn = db.connect()
    if a.cmd == "orch":
        return orch.dispatch(a, conn)
    {"submit": cmd_submit, "list": cmd_list, "status": cmd_status, "logs": cmd_logs, "cancel": cmd_cancel,
     "retry": cmd_retry, "usage": cmd_usage, "projects": cmd_projects, "doctor": cmd_doctor,
     "project": cmd_project_add}[a.cmd](a, conn)


if __name__ == "__main__":
    main()

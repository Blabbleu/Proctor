"""`swe` command-line interface: submit / status / list / logs / cancel / retry / usage / projects / doctor.

Works for root and members of group `swe`. Writes only to the job store; the supervisor does the work.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from swe_pilot import db, harness  # noqa: E402
from swe_pilot.common import (ACTIVE, JOB_ID_RE, STATE, TERMINAL, ProjectError, iso, list_projects,  # noqa: E402
                              load_project, load_settings, model_allowed, new_job_id, now, tail_text,
                              PROVIDER_LABEL, QUOTA_ROW, provider_for, role_model)
from swe_pilot.broker import quota_block_reason, remaining_allowance  # noqa: E402


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
            sys.exit("no spending ceiling configured in /etc/swe-pilot/settings.toml [budget]; refusing paid runs")
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
             f"usage:     {n} model calls, ≈ ${c:.3f} at list price (subscription-billed; gateway-metered)"]
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
    print(f"  quota guard (monitored threshold): pause model calls at 5h ≥ {g.get('max_5h'):.0%} or 7d ≥ {g.get('max_7d'):.0%}")
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
    ok = True

    def chk(name, cond, detail=""):
        nonlocal ok
        ok &= bool(cond)
        print(f"{'ok ' if cond else 'BAD'} {name}" + (f" — {detail}" if detail else ""))
    for svc in ("swe-supervisor", "swe-broker"):
        r = subprocess.run(["systemctl", "is-active", svc], capture_output=True, text=True)
        chk(f"service {svc}", r.stdout.strip() == "active", r.stdout.strip())
    s = load_settings()
    b = s["budget"]
    chk("spending ceiling configured", b.get("per_task_usd", 0) > 0 and b.get("per_day_usd", 0) > 0, f"task ${b.get('per_task_usd')} day ${b.get('per_day_usd')} month ${b.get('per_month_usd')}")
    a_ = conn.execute(f"SELECT COUNT(*) FROM jobs WHERE state IN ({','.join('?' * len(ACTIVE))})", ACTIVE).fetchone()[0]
    chk("at most one active job", a_ <= 1, f"{a_} active")
    for pid in list_projects():
        try:
            load_project(pid)
            chk(f"project {pid}", True)
        except Exception as e:
            chk(f"project {pid}", False, str(e))
    sys.exit(0 if ok else 1)


def main():
    ap = argparse.ArgumentParser(prog="swe", description="swe-pilot: autonomous coding tasks with independent validation")
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
    o = sp.add_parser("orch", help="orchestrator tools (gate/context/review/submit/merge/...); see `swe orch -h`")
    from swe_pilot import orch  # noqa: E402  (only the orch sub-commands depend on it)
    orch.add_parsers(o.add_subparsers(dest="orch_cmd", required=True, metavar="COMMAND"))
    a = ap.parse_args()
    os.umask(0o007)
    conn = db.connect()
    if a.cmd == "orch":
        return orch.dispatch(a, conn)
    {"submit": cmd_submit, "list": cmd_list, "status": cmd_status, "logs": cmd_logs, "cancel": cmd_cancel,
     "retry": cmd_retry, "usage": cmd_usage, "projects": cmd_projects, "doctor": cmd_doctor}[a.cmd](a, conn)


if __name__ == "__main__":
    main()

"""proctor supervisor: deterministic job state machine. Runs as user `proctor` under systemd.

No LLM decides anything here. Process exit codes, check exit codes and file contents produced
by trusted runner scripts drive every transition.
"""
from __future__ import annotations

import fcntl
import grp
import hashlib
import json
import logging
import os
import re
import secrets
import shutil
import signal
import datetime
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from proctor import db, harness  # noqa: E402
from proctor.common import (ACTIVE, APP, STATE, TERMINAL, ProjectError, db_meta_role, iso, load_project,  # noqa: E402
                              load_settings, now, path_protected, provider_for, read_small, role_paths,
                              role_violations, tail_text)
from proctor.broker import quota_block_reason, remaining_allowance  # noqa: E402

log = logging.getLogger("proctor-supervisor")
RUNNER = APP.parent / "runner"
STOP = False


class Blocked(Exception):
    def __init__(self, state, reason):
        super().__init__(reason)
        self.state, self.reason = state, reason


# --------------------------------------------------------------------------- docker helpers
def docker(*args, timeout=120, check=False, input=None):
    r = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout, input=input)
    if check and r.returncode != 0:
        raise RuntimeError(f"docker {args[0]} failed: {(r.stderr or r.stdout).strip()[-600:]}")
    return r


def inspect(name):
    if not name:
        return None
    r = docker("inspect", "--format", "{{json .State}}\t{{json .Config.Labels}}", name)
    if r.returncode != 0:
        return None
    st, labels = r.stdout.strip().split("\t", 1)
    return {"state": json.loads(st), "labels": json.loads(labels)}


def proctor_gid():
    return grp.getgrnam("proctor").gr_gid


def run_args(settings, project, name, labels, mounts, env, network, image, tmpfs_work=False):
    r = project["runner"]
    a = ["run", "--name", name, "--user", f"{settings['runner']['uid']}:{proctor_gid()}",
         "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--read-only",
         "--tmpfs", f"/tmp:rw,exec,nosuid,size={r['tmp_size']},mode=1777",
         "--memory", r["memory"], "--memory-swap", r["memory"], "--cpus", str(r["cpus"]),
         "--pids-limit", str(r["pids"]), "--log-opt", "max-size=5m", "--log-opt", "max-file=1",
         "--network", network, "--hostname", "proctor-runner", "--init"]
    if tmpfs_work:
        a += ["--tmpfs", f"/work:rw,exec,nosuid,size={r.get('work_tmpfs', '1500m')},uid={settings['runner']['uid']},gid={proctor_gid()},mode=2775"]
    for k, v in labels.items():
        a += ["--label", f"{k}={v}"]
    for src, dst, mode in mounts:
        a += ["-v", f"{src}:{dst}:{mode}"]
    base_env = {"HOME": "/home/proctor", "LANG": "C.UTF-8", "CI": "1"}
    base_env.update(r.get("env", {}))
    base_env.update(env)
    for k, v in base_env.items():
        a += ["-e", f"{k}={v}"]
    a.append(image)
    return a


def git(repo, *args, check=True, timeout=300, env=None, input=None):
    e = {"PATH": "/usr/bin:/bin", "HOME": str(STATE), "GIT_TERMINAL_PROMPT": "0", "LANG": "C.UTF-8"}
    e.update(env or {})
    r = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-c", "safe.directory=*", "--git-dir", str(repo), *args],
                       capture_output=True, text=True, timeout=timeout, env=e, input=input)
    if check and r.returncode != 0:
        raise RuntimeError(f"git {args[0]} failed: {r.stderr.strip()[-600:]}")
    return r


def failure_signature(failures):
    """Stable fingerprint of a validation failure: failing-test/error lines with numbers removed."""
    marks = re.compile(r"(FAILED|ERROR|Error|error|assert|✖|not ok|failed)")
    parts = []
    for f in failures:
        lines = [ln.strip() for ln in (f.get("tail") or "").splitlines()]
        hits = sorted({re.sub(r"[0-9]+(\.[0-9]+)?", "#", ln) for ln in lines if marks.search(ln) and len(ln) < 400})
        if not hits:
            hits = [re.sub(r"[0-9]+(\.[0-9]+)?", "#", ln) for ln in lines[-8:] if ln and not set(ln) <= set("_=- ")]
        parts.append((f["name"], f.get("status"), hits))
    return hashlib.sha256(json.dumps(sorted(parts, key=lambda p: p[0])).encode()).hexdigest()[:16]


# --------------------------------------------------------------------------- supervisor
class Supervisor:
    def __init__(self):
        self.conn = db.connect()
        db.init(self.conn)
        self.settings = load_settings()

    # ---------------------------------------------------------------- small utils
    def ev(self, job_id, kind, msg, **data):
        log.info("[%s] %s: %s", job_id, kind, msg)
        db.event(self.conn, job_id, kind, msg, **data)

    def upd(self, job_id, **cols):
        sets = ", ".join(f"{k}=?" for k in cols)
        self.conn.execute(f"UPDATE jobs SET {sets} WHERE id=?", (*cols.values(), job_id))

    def set_meta(self, job, **kv):
        m = db.meta(job)
        m.update(kv)
        self.upd(job["id"], meta=json.dumps(m))

    def jdir(self, job_id) -> Path:
        return STATE / "jobs" / job_id

    def mirror(self, pid) -> Path:
        return STATE / "repos" / f"{pid}.git"

    def notify(self, job_id, text):
        if self.conn.execute("SELECT notify FROM jobs WHERE id=?", (job_id,)).fetchone()[0]:
            self.conn.execute("INSERT INTO outbox(job_id, created_at, body) VALUES (?,?,?)", (job_id, now(), text))

    def mkdir(self, p: Path, mode=0o2775):
        p.mkdir(parents=True, exist_ok=True)
        os.chmod(p, mode)
        return p

    # ---------------------------------------------------------------- attempts
    def new_attempt(self, job, kind):
        seq = job["attempt_seq"] + 1
        aid = f"{job['id']}-a{seq}"
        self.conn.execute("INSERT INTO attempts(id, job_id, seq, round, kind, started_at) VALUES (?,?,?,?,?,?)",
                          (aid, job["id"], seq, job["round"], kind, now()))
        self.upd(job["id"], attempt_seq=seq, current_attempt=aid)
        return aid, seq

    def end_attempt(self, aid, status, exit_code=None, note=""):
        self.conn.execute("UPDATE attempts SET ended_at=?, status=?, exit_code=?, note=? WHERE id=? AND ended_at IS NULL",
                          (now(), status, exit_code, note[:2000], aid))

    def revoke_tokens(self, job_id):
        self.conn.execute("UPDATE tokens SET revoked=1 WHERE job_id=?", (job_id,))

    # ---------------------------------------------------------------- terminal transitions
    def finish(self, job_id, state, reason, preserve=True):
        job = db.job(self.conn, job_id)
        if job["state"] in TERMINAL:
            return
        self.revoke_tokens(job_id)
        if job["phase_container"]:
            docker("rm", "-f", job["phase_container"])
        if job["current_attempt"]:
            self.end_attempt(job["current_attempt"], "aborted" if state != "ready_for_review" else "done", note=reason)
        if preserve and state != "ready_for_review" and (self.jdir(job_id) / "work" / ".git").exists():
            try:
                p = self.export(job, tag="preserved")
                if p and p.stat().st_size:
                    self.ev(job_id, "preserve", f"work in progress saved to {p}", bytes=p.stat().st_size)
            except Exception as e:
                self.ev(job_id, "preserve", f"could not export work in progress: {e}")
        self.upd(job_id, state=state, reason=reason[:2000], finished_at=now(), phase_container=None,
                 phase_deadline=None, current_attempt=None)
        self.drop_snapshot(job_id)
        self.ev(job_id, "state", f"{state}: {reason}")
        job = db.job(self.conn, job_id)
        self.write_summary(job)
        self.notify(job_id, self.short_report(job))
        if state == "ready_for_review":
            self.cleanup_workspace(job_id)

    def pr_body(self, job):
        """Draft-PR text from supervisor evidence (not the agent's claims)."""
        lines = [f"**Goal:** {job['goal']}", ""]
        try:
            acc = json.loads(job["acceptance"] or "[]")
        except ValueError:
            acc = []
        if acc:
            lines += ["**Acceptance criteria:**"] + [f"- {a}" for a in acc] + [""]
        lines += [f"Base `{(job['base_commit'] or '')[:12]}` → candidate `{(job['candidate_commit'] or '')[:12]}` · "
                  f"harness {job['harness']} / {job['model']} · job `{job['id']}`", ""]
        lines += ["**Independent validation (supervisor, fresh offline container):**", self.check_line(job["id"])]
        return "\n".join(lines)

    def short_report(self, job):
        icon = {"ready_for_review": "✅", "cancelled": "⏹", "needs_input": "❓"}.get(job["state"], "⚠️")
        cost = self.conn.execute("SELECT COALESCE(SUM(cost_usd),0), COUNT(*) FROM usage WHERE job_id=?", (job["id"],)).fetchone()
        lines = [f"{icon} **proctor {job['id']}** · {job['project']} · {job['state']}", f"{job['title']}"]
        if job["state"] == "ready_for_review":
            pub = json.loads(job["publication"] or "{}")
            lines.append(f"Branch `{pub.get('branch')}` @ {(job['candidate_commit'] or '')[:10]} (base {(job['base_commit'] or '')[:10]})")
            lines.append(self.check_line(job["id"]))
        else:
            lines.append(f"Reason: {job['reason'][:300]}")
        lines.append(f"Model calls: {cost[1]} · usage ≈ ${cost[0]:.2f} list-price · `proctor status {job['id']}`")
        return "\n".join(lines)

    def check_line(self, job_id):
        stage = self.conn.execute("SELECT stage FROM checks WHERE job_id=? AND stage LIKE 'validate%' ORDER BY id DESC LIMIT 1",
                                  (job_id,)).fetchone()
        if not stage:
            return "Checks: none"
        rows = self.conn.execute("SELECT name, status, duration FROM checks WHERE job_id=? AND stage=? ORDER BY id",
                                 (job_id, stage[0])).fetchall()
        return "Checks: " + ", ".join(f"{r['name']} {r['status']}" for r in rows)

    def write_summary(self, job):
        d = self.jdir(job["id"]) / "artifacts"
        if not d.exists():
            return
        m = db.meta(job)
        rep = m.get("report") or {}
        lines = [f"# proctor {job['id']}: {job['title']}", "",
                 f"- Project: {job['project']}  ·  state: **{job['state']}**  ·  reason: {job['reason']}",
                 f"- Harness: {job['harness']} / {job['model']}  ·  rounds: {job['round']}  ·  attempts: {job['attempt_seq']}",
                 f"- Base: {job['base_branch']} @ {job['base_commit']}", f"- Candidate: {job['candidate_commit']}",
                 f"- Publication: {job['publication']}", "", "## Goal", job["goal"], "", "## Acceptance criteria"]
        lines += [f"- {a}" for a in json.loads(job["acceptance"])]
        lines += ["", "## Independent check evidence (supervisor-run)"]
        for r in self.conn.execute("SELECT * FROM checks WHERE job_id=? ORDER BY id", (job["id"],)):
            lines.append(f"- [{r['stage']}] {r['name']}: **{r['status']}** exit={r['exit_code']} {r['duration']:.1f}s "
                         f"commit={(r['commit_sha'] or '')[:10]} log={r['log_path']}")
        u = self.conn.execute("SELECT COUNT(*), COALESCE(SUM(input),0), COALESCE(SUM(output),0), COALESCE(SUM(cache_read),0), "
                              "COALESCE(SUM(cache_write),0), COALESCE(SUM(cost_usd),0) FROM usage WHERE job_id=?", (job["id"],)).fetchone()
        lines += ["", "## Model usage (gateway-metered, every call)",
                  f"{u[0]} calls · in {u[1]} · out {u[2]} · cache read {u[3]} · cache write {u[4]} · ≈ ${u[5]:.3f} at list price",
                  "", "## Worker report (agent's own claims — not evidence)",
                  f"- status: {rep.get('status')}", f"- summary: {rep.get('summary')}"]
        lines += [f"- claimed check: {c}" for c in rep.get("checks_run", [])[:20]]
        lines += [f"- limitation: {c}" for c in rep.get("limitations", [])[:20]]
        lines += [f"- question: {c}" for c in rep.get("questions", [])[:20]]
        (d / "SUMMARY.md").write_text("\n".join(lines) + "\n")

    def drop_snapshot(self, job_id):
        """The [data] copy is disposable: gone as soon as the job is terminal."""
        snap = self.jdir(job_id) / "data-snapshot"
        if snap.exists():
            shutil.rmtree(snap, ignore_errors=True)
            if snap.exists():
                self.root_rm(snap)

    def cleanup_workspace(self, job_id):
        jd = self.jdir(job_id)
        self.drop_snapshot(job_id)
        for sub in ("work", "home"):
            p = jd / sub
            if p.exists():
                self.root_rm(p)

    def root_rm(self, p: Path):
        """Remove a runner-owned tree with a throwaway container (runner uid may own files)."""
        image = f"proctor-base:{self.base_tag()}"
        docker("run", "--rm", "--network", "none", "--cap-drop", "ALL", "--cap-add", "DAC_OVERRIDE", "--cap-add", "FOWNER",
               "--user", "0:0", "-v", f"{p.parent}:/p", image, "rm", "-rf", f"/p/{p.name}", timeout=300)

    # ---------------------------------------------------------------- images
    def base_tag(self):
        h = hashlib.sha256()
        for f in sorted((APP.parent / "images" / "base").rglob("*")) + sorted(RUNNER.rglob("*")):
            if f.is_file():
                h.update(f.name.encode())
                h.update(f.read_bytes())
        h.update(Path(self.settings["harness"]["claude_binary"]).name.encode())
        if self.settings["harness"].get("codex_binary"):
            h.update(Path(self.settings["harness"]["codex_binary"]).name.encode())
            h.update(b"code-mode-host")
        return h.hexdigest()[:12]

    def ensure_base(self):
        tag = f"proctor-base:{self.base_tag()}"
        if docker("image", "inspect", tag).returncode == 0:
            return tag
        ctx = STATE / "build" / "base"
        if ctx.exists():
            shutil.rmtree(ctx)
        shutil.copytree(APP.parent / "images" / "base", ctx)
        shutil.copytree(RUNNER, ctx / "runner")
        shutil.copy2(self.settings["harness"]["claude_binary"], ctx / "claude")
        cx = self.settings["harness"].get("codex_binary")
        if cx:
            shutil.copy2(cx, ctx / "codex")
            helper = Path(cx).with_name(Path(cx).name.replace("codex-", "codex-code-mode-host-", 1))
            if helper.exists():
                shutil.copy2(helper, ctx / "codex-code-mode-host")
            else:
                (ctx / "codex-code-mode-host").write_text("#!/bin/sh\nexit 127\n")
        else:
            (ctx / "codex-code-mode-host").write_text("#!/bin/sh\nexit 127\n")
            (ctx / "codex").write_text("#!/bin/sh\necho 'codex harness not installed' >&2; exit 127\n")
        r = docker("build", "-q", "-t", tag, str(ctx), timeout=1800)
        shutil.rmtree(ctx, ignore_errors=True)
        if r.returncode:
            raise Blocked("blocked_environment", f"base image build failed: {r.stderr[-800:]}")
        return tag

    def ensure_project_image(self, job, project):
        base = self.ensure_base()
        pdir = Path(project["_dir"])
        h = hashlib.sha256(base.encode())
        files = {}
        for rel in project["runner"].get("context_files", []):
            r = git(self.mirror(project["id"]), "show", f"{job['base_commit']}:{rel}", check=False)
            if r.returncode:
                raise Blocked("blocked_environment", f"context file {rel} missing at base commit")
            files[rel] = r.stdout
            h.update(rel.encode() + r.stdout.encode())
        for f in sorted(pdir.glob("*")):
            if f.is_file() and f.name != "project.toml":
                h.update(f.name.encode() + f.read_bytes())
        tag = f"proctor-{project['id']}:{h.hexdigest()[:12]}"
        if docker("image", "inspect", tag).returncode == 0:
            return tag
        self.ev(job["id"], "image", f"building runner image {tag}")
        ctx = STATE / "build" / project["id"]
        if ctx.exists():
            shutil.rmtree(ctx)
        ctx.mkdir(parents=True)
        for f in pdir.glob("*"):
            if f.is_file() and f.name != "project.toml":
                shutil.copy2(f, ctx / f.name)
        for rel, content in files.items():
            dst = ctx / "ctx" / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_text(content)
        r = docker("build", "-q", "--build-arg", f"BASE={base}", "-t", tag, str(ctx), timeout=1800)
        shutil.rmtree(ctx, ignore_errors=True)
        if r.returncode:
            raise Blocked("blocked_environment", f"runner image build failed: {r.stderr[-1500:]}")
        return tag

    # ---------------------------------------------------------------- blocking helper containers
    def run_blocking(self, job, project, phase, mode, mounts, env, network="none", timeout=600, tmpfs_work=False):
        name = f"proctor-{job['id']}-{phase}"
        docker("rm", "-f", name)
        args = run_args(self.settings, project, name, {"proctor.job": job["id"], "proctor.phase": phase, "proctor.blocking": "1"},
                        mounts, env, network, job["image"], tmpfs_work=tmpfs_work)
        args.insert(1, "--rm")
        args += ["bash", "/opt/proctor/run.sh", mode]
        r = docker(*args, timeout=timeout)
        return r.returncode, (r.stdout + r.stderr)[-2000:]

    def export(self, job, tag):
        """Export the worker tree as a patch against the base commit, computed in a fresh container."""
        project = load_project(job["project"])
        jd = self.jdir(job["id"])
        out = self.mkdir(jd / "out" / f"export-{tag}")
        for f in out.iterdir():
            if f.is_file():
                f.unlink()
        rc, msg = self.run_blocking(job, project, f"export-{tag}", "export",
                                    [(str(jd / "work"), "/w", "ro"), (str(self.mirror(job["project"])), "/mirror", "ro"),
                                     (str(jd / "in"), "/job/in", "ro"), (str(out), "/job/out", "rw")],
                                    {"PROCTOR_BASE": job["base_commit"], "HOME": "/tmp"}, timeout=300)
        if rc != 0:
            raise RuntimeError(f"export failed (exit {rc}): {msg}")
        art = self.mkdir(jd / "artifacts")
        dst = art / f"{tag}.patch"
        data = (read_small(out / "candidate.patch", 64 * 1024 * 1024) or "")
        dst.write_text(data)
        for extra in ("files.txt", "stat.txt"):
            t = read_small(out / extra) or ""
            (art / f"{tag}.{extra}").write_text(t)
        return dst

    # ---------------------------------------------------------------- detached phase containers
    def launch(self, job, project, phase, mode, mounts, env, network="none", minutes=30, tmpfs_work=False, kind=None):
        aid, seq = self.new_attempt(job, kind or phase)
        name = f"proctor-{job['id']}-{seq}-{phase}"
        labels = {"proctor.job": job["id"], "proctor.attempt": aid, "proctor.phase": phase}
        args = run_args(self.settings, project, name, labels, mounts, {**env, "PROCTOR_ATTEMPT": aid}, network, job["image"],
                        tmpfs_work=tmpfs_work)
        args.insert(1, "-d")
        args += ["bash", "/opt/proctor/run.sh", mode]
        deadline = min(job["deadline"] or now() + minutes * 60, now() + minutes * 60)
        r = docker(*args, timeout=120)
        if r.returncode:
            self.end_attempt(aid, "error", note=r.stderr[-500:])
            raise RuntimeError(f"container start failed: {r.stderr.strip()[-500:]}")
        self.conn.execute("UPDATE attempts SET container=? WHERE id=?", (name, aid))
        self.upd(job["id"], phase_container=name, phase_started_at=now(), phase_deadline=deadline, kill_reason=None)
        self.ev(job["id"], "launch", f"{phase} container {name} (attempt {aid})", deadline=iso(deadline))
        return aid

    def data_mounts(self, job, project, checks):
        """Mounts/env for the [data] snapshot: copies at /job/data (ro) plus the read-only source paths. Workers get the
        real bind at /work/<path>; check containers get /job/ro/<n>, which run.sh links into the tmpfs checkout."""
        if not (project["data"]["copy"] or project["data"]["mount_ro"]):
            return [], {}
        dm = db.meta(job).get("data") or {}
        mounts, ro_map = [], []
        if dm.get("snapshot") and Path(dm["snapshot"]).is_dir():
            mounts.append((dm["snapshot"], "/job/data", "ro"))
        for i, m in enumerate(dm.get("mount_ro", [])):
            if checks:
                mounts.append((m["src"], f"/job/ro/{i}", "ro"))
                ro_map.append(f"{m['rel']}={i}")
            else:
                mounts.append((m["src"], f"/work/{m['rel']}", "ro"))
        return mounts, ({"PROCTOR_DATA": "1", "PROCTOR_RO_MAP": "|".join(ro_map)} if checks else {})

    def prepare_mountpoints(self, project, work: Path):
        """Create the proctor-owned mount targets of [data].mount_ro in the task checkout (docker would make them root-owned)."""
        base = work.resolve()
        for m in project["data"]["mount_ro"]:
            tgt = (work / m).resolve()
            if tgt != base and base not in tgt.parents:
                raise Blocked("blocked_environment", f"[data].mount_ro {m!r} resolves outside the checkout")
            if not tgt.exists():
                tgt.mkdir(parents=True, exist_ok=True)
                os.chmod(tgt, 0o2775)

    def checks_spec(self, job, project, stage):
        jd = self.jdir(job["id"])
        out = self.mkdir(jd / "out" / stage)
        for f in out.rglob("*"):
            if f.is_file():
                f.unlink()
        self.mkdir(out / "logs")
        inp = jd / "in" / f"checks-{stage}.json"
        inp.write_text(json.dumps({"prepare": project["prepare"], "checks": project["checks"]}))
        os.chmod(inp, 0o664)
        shutil.copy2(inp, jd / "in" / "checks.json")
        return out

    def launch_checks(self, job, project, stage, commit):
        out = self.checks_spec(job, project, stage)
        jd = self.jdir(job["id"])
        stage_in = self.mkdir(jd / "in-stage" / stage)
        shutil.copy2(jd / "in" / f"checks-{stage}.json", stage_in / "checks.json")
        os.chmod(stage_in / "checks.json", 0o664)
        minutes = max(5, sum(c["timeout"] for c in project["checks"]) / 60 + 10)
        self.upd(job["id"], step=stage.split("-")[0])
        mounts = [(str(self.mirror(job["project"])), "/mirror", "ro"), (str(stage_in), "/job/in", "ro"),
                  (str(out), "/job/out", "rw")]
        env = {"PROCTOR_COMMIT": commit, "PROCTOR_BASE_COMMIT": job["base_commit"], "PROCTOR_BRANCH": f"proctor/{job['id']}", "HOME": "/tmp"}
        if any(c.get("data") for c in project["checks"]):
            dmounts, denv = self.data_mounts(job, project, checks=True)
            mounts += dmounts
            env.update(denv)
        return self.launch(job, project, stage, "checks", mounts, env, network="none",
                           minutes=minutes, tmpfs_work=True, kind=stage)

    def launch_worker(self, job, project, prompt, resume):
        jd = self.jdir(job["id"])
        adapter = harness.ADAPTERS[job["harness"]]
        winp = self.mkdir(jd / "in-worker" / f"r{job['round']}")
        (winp / "prompt.md").write_text(prompt)
        os.chmod(winp / "prompt.md", 0o664)
        shutil.copy2(jd / "in" / "export-exclude.txt", winp / "export-exclude.txt")
        (winp / "prepare.sh").write_text("set -e\n" + "\n".join(project["prepare"]) + "\n")
        os.chmod(winp / "prepare.sh", 0o664)
        out = self.mkdir(jd / "out" / f"worker-r{job['round']}")
        for f in out.iterdir():
            if f.is_file():
                f.unlink()
        self.mkdir(jd / "home")
        job = db.job(self.conn, job["id"])
        state = "implementing" if job["round"] == 0 else "repairing"
        self.upd(job["id"], state=state, step="worker")
        job = db.job(self.conn, job["id"])
        # The token is minted for the attempt id the launch will create next.
        token = "proctor-" + secrets.token_urlsafe(32)
        next_aid = f"{job['id']}-a{job['attempt_seq'] + 1}"
        self.conn.execute("INSERT INTO tokens(hash, job_id, attempt_id, created_at) VALUES (?,?,?,?)",
                          (hashlib.sha256(token.encode()).hexdigest(), job["id"], next_aid, now()))
        left, _ = remaining_allowance(self.conn, self.settings, job["id"])
        env = {"PROCTOR_HARNESS": job["harness"], "PROCTOR_MODEL": job["model"] or "", "PROCTOR_SESSION": job["session_id"] or "",
               "PROCTOR_MAX_TURNS": str(project["limits"]["max_turns"]), "PROCTOR_REPORT": "/job/out/report.json",
               "PROCTOR_CHECKPOINT": "/job/out/checkpoint.log", "PROCTOR_MOCK": db.meta(job).get("mock", "good"),
               "PROCTOR_ROUND": str(job["round"]), "PROCTOR_MAX_BUDGET": f"{max(left, 0.01):.2f}" if adapter.network else ""}
        if resume and adapter.resumable and job["session_id"]:
            env["PROCTOR_RESUME"] = job["session_id"]
        env.update(adapter.env(job, next_aid, token, self.settings))
        network = self.settings["gateway"]["network"] if adapter.network else "none"
        minutes = project["limits"]["worker_minutes"]
        mounts = [(str(jd / "work"), "/work", "rw"), (str(jd / "home"), "/home/proctor", "rw"),
                  (str(winp), "/job/in", "ro"), (str(out), "/job/out", "rw")]
        mounts += self.data_mounts(job, project, checks=False)[0]
        return self.launch(job, project, f"worker-r{job['round']}", "worker", mounts,
                           env, network=network, minutes=minutes, kind="initial" if job["round"] == 0 else "repair")

    # ---------------------------------------------------------------- broker requests
    def request(self, kind, job_id, payload):
        cur = self.conn.execute("INSERT INTO requests(kind, job_id, payload, created_at) VALUES (?,?,?,?)",
                                (kind, job_id, json.dumps(payload), now()))
        return cur.lastrowid

    def request_result(self, rid):
        r = self.conn.execute("SELECT * FROM requests WHERE id=?", (rid,)).fetchone()
        if r is None or r["status"] in ("pending", "running"):
            return None, r
        return json.loads(r["result"] or "{}"), r

    # ---------------------------------------------------------------- the state machine
    def start_job(self, job):
        project = load_project(job["project"])
        free = shutil.disk_usage(STATE).free / 2**30
        if free < self.settings["runner"]["min_free_disk_gb"]:
            raise Blocked("blocked_environment", f"only {free:.1f} GiB free under {STATE}")
        if harness.ADAPTERS[job["harness"]].network:
            q = quota_block_reason(self.conn, self.settings, provider_for(job["model"]))
            if q:
                if db.meta(job).get("quota_wait") != q:
                    self.set_meta(job, quota_wait=q)
                    self.ev(job["id"], "wait", f"waiting for subscription quota: {q}")
                return False
            left, parts = remaining_allowance(self.conn, self.settings, job["id"])
            if left <= 0.05:
                raise Blocked("blocked_budget", f"allowance exhausted before start: {parts}")
        t = now()
        lim = project["limits"]
        self.upd(job["id"], state="preparing", step="sync", started_at=t, deadline=t + lim["task_minutes"] * 60,
                 base_branch=project["base_branch"])
        rid = self.request("sync", job["id"], {"project": project["id"]})
        self.set_meta(db.job(self.conn, job["id"]), sync_req=rid)
        self.ev(job["id"], "state", f"preparing: syncing {project['base_branch']} from registered source")
        return True

    def step_sync(self, job, project):
        m = db.meta(job)
        res, row = self.request_result(m["sync_req"])
        if res is None:
            if now() - row["created_at"] > 180:
                raise Blocked("blocked_environment", "repository broker did not answer the sync request")
            return
        if row["status"] != "done":
            raise Blocked("blocked_environment", f"sync failed: {res.get('error')}")
        if (project["data"]["copy"] or project["data"]["mount_ro"]) and "data" not in m:
            if not self.step_snapshot(job, project):
                return
            job = db.job(self.conn, job["id"])
        mirror = self.mirror(project["id"])
        if not mirror.exists():
            subprocess.run(["git", "init", "-q", "--bare", str(mirror)], check=True)
        git(mirror, "fetch", "-q", "--no-tags", res["bundle"], f"+refs/heads/{project['base_branch']}:refs/remotes/source/{project['base_branch']}")
        head = git(mirror, "rev-parse", f"refs/remotes/source/{project['base_branch']}").stdout.strip()
        if head != res["head"]:
            raise Blocked("blocked_environment", "bundle head does not match reported source head")
        self.upd(job["id"], base_commit=head)
        job = db.job(self.conn, job["id"])
        self.set_meta(job, source_remote=res.get("remote"))
        self.ev(job["id"], "base", f"base commit {head} ({project['base_branch']})")
        image = self.ensure_project_image(job, project)
        self.upd(job["id"], image=image)
        job = db.job(self.conn, job["id"])
        jd = self.jdir(job["id"])
        for sub in ("in", "out", "artifacts"):
            self.mkdir(jd / sub)
        excl = list(project["runner"].get("export_exclude", [])) + ["/.proctor-*"]
        excl += ["/" + e.strip("/") for e in project["data"]["copy"] + project["data"]["mount_ro"]]
        (jd / "in" / "export-exclude.txt").write_text("\n".join(excl) + "\n")
        (jd / "in" / "task.json").write_text(json.dumps({k: job[k] for k in ("id", "project", "title", "goal", "acceptance")}))
        work = jd / "work"
        if work.exists():
            self.root_rm(work)
        self.mkdir(work)
        rc, msg = self.run_blocking(job, project, "clone", "clone",
                                    [(str(self.mirror(project["id"])), "/mirror", "ro"), (str(work), "/work", "rw"),
                                     (str(self.mkdir(jd / "out" / "clone")), "/job/out", "rw")],
                                    {"PROCTOR_COMMIT": head, "PROCTOR_BRANCH": f"proctor/{job['id']}", "HOME": "/tmp"}, timeout=300)
        if rc:
            raise Blocked("blocked_environment", f"workspace clone failed (exit {rc}): {msg}")
        self.prepare_mountpoints(project, work)
        self.ev(job["id"], "workspace", f"task checkout ready at base {head[:12]}")
        self.launch_checks(job, project, "baseline", head)

    def step_snapshot(self, job, project):
        """Ask the broker for the disposable [data] copy; True once it is recorded in the job meta."""
        m = db.meta(job)
        if "snapshot_req" not in m:
            rid = self.request("snapshot", job["id"], {"project": project["id"]})
            self.set_meta(job, snapshot_req=rid)
            self.ev(job["id"], "data", f"data snapshot request {rid}")
            return False
        res, row = self.request_result(m["snapshot_req"])
        if res is None:
            if now() - row["created_at"] > 600:
                raise Blocked("blocked_environment", "repository broker did not answer the data snapshot request")
            return False
        if row["status"] != "done":
            raise Blocked("blocked_environment", f"data snapshot failed: {res.get('error')}")
        self.set_meta(job, data={"snapshot": res.get("path"), "mount_ro": res.get("mount_ro", []),
                                 "bytes": res.get("bytes", 0), "notes": res.get("notes", [])})
        self.ev(job["id"], "data", f"data snapshot ready: {len(res.get('copied', []))} entries, "
                f"{res.get('bytes', 0) / 2**20:.1f} MiB, {len(res.get('mount_ro', []))} read-only mounts", notes=res.get("notes"))
        return True

    def collect_checks(self, job, stage, commit):
        out = self.jdir(job["id"]) / "out" / stage
        raw = read_small(out / "checks.json")
        if not raw:
            return None
        res = json.loads(raw)
        art = self.mkdir(self.jdir(job["id"]) / "artifacts" / "checks" / stage)
        rows = []
        for p in res.get("prepare", []):
            if p["status"] != "pass":
                rows.append({"name": "prepare", **p})
        for c in res.get("checks", []):
            src = out / c["log"]
            dst = art / f"{c['name']}.log"
            dst.write_text(tail_text(src, 1_000_000))
            self.conn.execute("INSERT INTO checks(job_id, stage, name, command, required, commit_sha, exit_code, duration, status, log_path, ts)"
                              " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                              (job["id"], stage, c["name"], c["command"], int(c["required"]), res.get("commit"), c["exit"],
                               c["seconds"], c["status"], str(dst), now()))
            c["tail"] = tail_text(src, 4000)
            rows.append(c)
        if res.get("commit") != commit:
            raise RuntimeError(f"check report is for {res.get('commit')}, expected {commit}")
        res["rows"] = rows
        return res

    def step_container(self, job, project):
        c = inspect(job["phase_container"])
        if c is None:
            return self.recover_missing(job, project)
        if c["labels"].get("proctor.attempt") != job["current_attempt"]:
            self.ev(job["id"], "stale", f"ignoring container {job['phase_container']} from attempt {c['labels'].get('proctor.attempt')}")
            docker("rm", "-f", job["phase_container"])
            return self.recover_missing(job, project)
        st = c["state"]
        if st.get("Running"):
            if now() > (job["phase_deadline"] or 1e18):
                reason = "task time limit reached" if now() >= (job["deadline"] or 1e18) else "phase time limit reached"
                self.upd(job["id"], kill_reason=reason)
                docker("kill", job["phase_container"])
                self.ev(job["id"], "timeout", f"{reason}; killed {job['phase_container']}")
            return
        exit_code = st.get("ExitCode")
        oom = st.get("OOMKilled")
        job = db.job(self.conn, job["id"])
        if self.killed_by_infra(job, st, exit_code, oom):
            # dockerd/host restart stopped the container (SIGTERM/SIGKILL) without us asking: the
            # phase did not finish, so its output must not be judged. Treat it like a lost container.
            self.ev(job["id"], "recover", f"{job['phase_container']} was stopped by a docker/host restart "
                                          f"(exit {exit_code}); not treating it as finished")
            docker("rm", "-f", job["phase_container"])
            return self.recover_missing(job, project)
        docker("rm", "-f", job["phase_container"])
        aid = job["current_attempt"]
        self.end_attempt(aid, "oom" if oom else ("timeout" if job["kill_reason"] else ("ok" if exit_code == 0 else "exit")),
                         exit_code, note=job["kill_reason"] or "")
        self.upd(job["id"], phase_container=None)
        job = db.job(self.conn, job["id"])
        phase = job["step"]
        if job["kill_reason"]:
            if phase == "worker" and job["kill_reason"].startswith("task"):
                raise Blocked("failed", f"{job['kill_reason']} during {job['state']}")
            if phase == "worker":
                raise Blocked("failed", f"worker exceeded {project['limits']['worker_minutes']} min")
            raise Blocked("blocked_environment", f"{phase} checks exceeded their time limit")
        if oom:
            raise Blocked("blocked_environment", f"{phase} container hit the {project['runner']['memory']} memory limit "
                                                 f"(OOM); raise runner.memory only if the host has headroom")
        getattr(self, f"after_{phase}")(job, project, exit_code)

    def after_baseline(self, job, project, exit_code):
        res = self.collect_checks(job, "baseline", job["base_commit"])
        if res is None:
            raise Blocked("blocked_environment", f"baseline produced no report (exit {exit_code})")
        if not res.get("prepare_ok", True):
            raise Blocked("blocked_environment", "baseline prepare step failed: " +
                          "; ".join(f"{p['command']} exit {p['exit']}" for p in res["prepare"] if p["status"] != "pass"))
        bad = [c for c in res["checks"] if c["required"] and c["status"] in ("error", "oom", "timeout")]
        if bad:
            raise Blocked("blocked_environment", "baseline check environment problem: " +
                          ", ".join(f"{c['name']} {c['status']}" for c in bad))
        baseline = {c["name"]: c["status"] for c in res["checks"]}
        job = db.job(self.conn, job["id"])
        self.set_meta(job, baseline=baseline, baseline_rows=[{k: c[k] for k in ("name", "status", "exit", "seconds")} for c in res["checks"]])
        pre = [n for n, s in baseline.items() if s != "pass"]
        self.ev(job["id"], "baseline", "baseline " + ", ".join(f"{n}={s}" for n, s in baseline.items()),
                preexisting_failures=pre)
        job = db.job(self.conn, job["id"])
        prompt = harness.task_prompt(job, project, db.meta(job)["baseline_rows"])
        if harness.ADAPTERS[job["harness"]].resumable and not job["session_id"]:
            import uuid
            # Claude Code accepts a caller-chosen session id; Codex reports its own thread id, which
            # after_worker stores for resume.
            sid = {"claude-code": str(uuid.uuid4()), "mock": "mock"}.get(job["harness"])
            if sid:
                self.upd(job["id"], session_id=sid)
                job = db.job(self.conn, job["id"])
        self.launch_worker(job, project, prompt, resume=False)

    def after_worker(self, job, project, exit_code):
        jd = self.jdir(job["id"])
        out = jd / "out" / f"worker-r{job['round']}"
        adapter = harness.ADAPTERS[job["harness"]]
        parsed = adapter.parse(out)
        art = self.mkdir(jd / "artifacts")
        shutil.copyfile(out / "worker.jsonl", art / f"worker-r{job['round']}.jsonl") if (out / "worker.jsonl").exists() else None
        (art / f"worker-r{job['round']}.stderr").write_text(tail_text(out / "worker.stderr", 200_000))
        ck = read_small(out / "checkpoint.log", 200_000)
        if ck:
            with open(art / "checkpoints.log", "a") as fh:
                fh.write(f"--- round {job['round']}\n{ck}\n")
        report = {}
        raw = read_small(out / "report.json", 200_000)
        if raw:
            try:
                report = json.loads(raw)
                if not isinstance(report, dict):
                    report = {}
            except ValueError:
                report = {"status": "unparseable"}
        attempt = job["current_attempt"]
        if parsed.get("session_id") and adapter.resumable and job["harness"] != "mock":
            self.upd(job["id"], session_id=parsed["session_id"])
        self.conn.execute("UPDATE attempts SET note=? WHERE id=?",
                          (json.dumps({k: parsed.get(k) for k in ("subtype", "is_error", "reported_cost_usd", "num_turns", "events")}), attempt))
        job = db.job(self.conn, job["id"])
        self.set_meta(job, report=report, worker=parsed)
        self.ev(job["id"], "worker", f"worker exited {exit_code} ({parsed.get('subtype')}); report status {report.get('status')}",
                reported_cost=parsed.get("reported_cost_usd"))
        blocks = self.conn.execute("SELECT kind, reason FROM gateway_blocks WHERE attempt_id=? ORDER BY id DESC LIMIT 1",
                                   (attempt,)).fetchone()
        patch = self.export(job, tag=f"round{job['round']}")
        size = patch.stat().st_size
        if blocks and blocks["kind"] == "quota":
            return self.pause_for_quota(job, blocks["reason"])
        if blocks and blocks["kind"] == "budget":
            raise Blocked("blocked_budget", f"gateway refused model call ({blocks['kind']}): {blocks['reason']}")
        if blocks and blocks["kind"] in ("auth", "model"):
            raise Blocked("blocked_environment", f"gateway refused model call ({blocks['kind']}): {blocks['reason']}")
        status = report.get("status")
        if size == 0:
            if status == "needs_input":
                raise Blocked("needs_input", "worker needs input: " + "; ".join(report.get("questions", [])[:5]))
            if status == "blocked":
                raise Blocked("blocked_environment", "worker reported blocked: " + str(report.get("summary"))[:500])
            raise Blocked("failed", f"worker produced no change (exit {exit_code}, {parsed.get('subtype')})")
        if exit_code != 0 and parsed.get("subtype") not in ("success", "error_max_turns", "mock"):
            self.ev(job["id"], "worker", f"worker exit {exit_code} but produced a change; validating it anyway")
        if size > project["limits"]["max_patch_kb"] * 1024:
            raise Blocked("failed", f"candidate patch is {size // 1024} KiB (limit {project['limits']['max_patch_kb']} KiB)")
        files = [ln.split("\t")[-1] for ln in (jd / "artifacts" / f"round{job['round']}.files.txt").read_text().splitlines() if ln]
        prot = [f for f in files if path_protected(f, project["protected_paths"])]
        if prot:
            raise Blocked("needs_input", f"change touches protected paths {prot[:5]}; review manually (patch kept)")
        rname = db_meta_role(job)
        if rname:
            if rname not in project["roles"]:
                raise Blocked("failed", f"role {rname!r} is no longer defined for {project['id']}")
            # name-status lines are "M\tpath" or "R100\told\tnew": a rename touches both sides
            touched = [x for ln in (jd / "artifacts" / f"round{job['round']}.files.txt").read_text().splitlines() if ln
                       for x in ln.split("\t")[1:]]
            bad = role_violations(project, rname, touched)
            if bad:
                return self.role_scope_violation(job, project, rname, bad)
        sha, tree = self.freeze_candidate(job, patch)
        self.upd(job["id"], candidate_commit=sha, state="validating")
        job = db.job(self.conn, job["id"])
        self.set_meta(job, candidate_tree=tree)
        self.ev(job["id"], "candidate", f"frozen candidate {sha} ({len(files)} files, {size} bytes)")
        self.launch_checks(db.job(self.conn, job["id"]), project, f"validate-r{job['round']}", sha)

    def role_scope_violation(self, job, project, rname, bad):
        """The change touches files outside the role's paths: a worker mistake, so repair (if rounds remain) or fail."""
        allowed = role_paths(project, rname)
        shown = bad[:20]
        sig = failure_signature([{"name": "role-scope", "status": "fail", "tail": "\n".join(sorted(bad))}])
        self.ev(job["id"], "role-scope", f"{len(bad)} file(s) outside the {rname} role's paths: {shown}", signature=sig,
                allowed=allowed)
        lim = project["limits"]
        why = f"role scope: {len(bad)} file(s) outside the {rname} role's paths {shown}"
        if job["round"] >= lim["repair_rounds"]:
            raise Blocked("failed", f"{why} (after {job['round']} repair round(s))")
        if job["failure_sig"] == sig:
            raise Blocked("failed", f"{why} (same violation repeated without progress)")
        if (job["deadline"] or 0) - now() < 5 * 60:
            raise Blocked("failed", f"{why} (<5 min of task time remains)")
        left, _ = remaining_allowance(self.conn, self.settings, job["id"]) if harness.ADAPTERS[job["harness"]].network else (1e9, None)
        if left < 0.25:
            raise Blocked("blocked_budget", f"{why}; the remaining allowance (${left:.2f}) is too small to repair")
        self.upd(job["id"], round=job["round"] + 1, failure_sig=sig)
        job = db.job(self.conn, job["id"])
        prompt = harness.role_repair_prompt(job, rname, allowed, bad, job["round"], lim["repair_rounds"])
        self.launch_worker(job, project, prompt, resume=True)

    def freeze_candidate(self, job, patch: Path):
        """Apply the exported patch to the base in the trusted mirror without a working tree."""
        mirror = self.mirror(job["project"])
        idx = self.jdir(job["id"]) / "candidate.index"
        idx.unlink(missing_ok=True)
        env = {"GIT_INDEX_FILE": str(idx)}
        git(mirror, "read-tree", job["base_commit"], env=env)
        r = git(mirror, "apply", "--cached", "--binary", "--whitespace=nowarn", str(patch), env=env, check=False)
        if r.returncode:
            raise Blocked("failed", f"candidate patch does not apply cleanly to base: {r.stderr[-400:]}")
        tree = git(mirror, "write-tree", env=env).stdout.strip()
        idx.unlink(missing_ok=True)
        msg = f"proctor {job['id']}: {job['title']}\n\nTask: {job['goal'][:1500]}\n\nGenerated by proctor ({job['harness']}/{job['model']}), round {job['round']}.\n"
        cenv = {"GIT_AUTHOR_NAME": "proctor", "GIT_AUTHOR_EMAIL": "proctor@localhost",
                "GIT_COMMITTER_NAME": "proctor", "GIT_COMMITTER_EMAIL": "proctor@localhost"}
        sha = git(mirror, "commit-tree", tree, "-p", job["base_commit"], env=cenv, input=msg).stdout.strip()
        git(mirror, "update-ref", f"refs/proctor/{job['id']}/r{job['round']}", sha)
        return sha, tree

    def after_validate(self, job, project, exit_code):
        stage = f"validate-r{job['round']}"
        res = self.collect_checks(job, stage, job["candidate_commit"])
        if res is None:
            raise Blocked("blocked_environment", f"validation produced no report (exit {exit_code})")
        baseline = db.meta(job).get("baseline", {})
        failures = []
        if not res.get("prepare_ok", True):
            failures += [{"name": "prepare", "command": p["command"], "status": p["status"], "exit": p["exit"], "tail": ""}
                         for p in res["prepare"] if p["status"] != "pass"]
        rname = db_meta_role(job)
        role_checks = set((project["roles"].get(rname) or {}).get("checks") or []) if rname else set()
        for c in res["checks"]:
            if not (c["required"] or c["name"] in role_checks):
                continue
            if c["status"] != "pass" and baseline.get(c["name"]) == "pass":
                failures.append(c)
        pre = [c["name"] for c in res["checks"] if c["status"] != "pass" and baseline.get(c["name"]) != "pass"]
        if not failures:
            self.ev(job["id"], "validate", f"all required checks pass on {job['candidate_commit'][:12]}"
                    + (f"; pre-existing failures unchanged: {pre}" if pre else ""))
            self.upd(job["id"], state="publishing", step="publish")
            return
        sig = failure_signature(failures)
        names = ", ".join(f"{f['name']} {f['status']}" for f in failures)
        self.ev(job["id"], "validate", f"validation failed: {names}", signature=sig)
        lim = project["limits"]
        left, _ = remaining_allowance(self.conn, self.settings, job["id"]) if harness.ADAPTERS[job["harness"]].network else (1e9, None)
        if job["round"] >= lim["repair_rounds"]:
            raise Blocked("failed", f"validation still failing after {job['round']} repair round(s): {names}")
        if job["failure_sig"] == sig:
            raise Blocked("failed", f"same failure repeated without progress: {names}")
        if (job["deadline"] or 0) - now() < 5 * 60:
            raise Blocked("failed", f"validation failed and <5 min of task time remains: {names}")
        if left < 0.25:
            raise Blocked("blocked_budget", f"validation failed and the remaining allowance (${left:.2f}) is too small to repair")
        self.upd(job["id"], round=job["round"] + 1, failure_sig=sig)
        job = db.job(self.conn, job["id"])
        prompt = harness.repair_prompt(job, failures, job["round"], lim["repair_rounds"])
        self.launch_worker(job, project, prompt, resume=True)

    def step_publish(self, job, project):
        m = db.meta(job)
        if job["publication"]:
            pub = json.loads(job["publication"])
            if pub.get("done"):
                return self.finish(job["id"], "ready_for_review", "required checks pass on the frozen candidate")
        mirror = self.mirror(project["id"])
        branch = f"proctor/{job['id']}"
        art = self.mkdir(self.jdir(job["id"]) / "artifacts")
        final = art / "candidate.patch"
        if not final.exists():
            r = git(mirror, "format-patch", "--stdout", "--binary", f"{job['base_commit']}..{job['candidate_commit']}")
            final.write_text(r.stdout)
        if "publish_req" not in m:
            tree = git(mirror, "rev-parse", f"{job['candidate_commit']}^{{tree}}").stdout.strip()
            if tree != m.get("candidate_tree"):
                raise Blocked("failed", "candidate tree changed after validation; refusing to publish")
            git(mirror, "update-ref", f"refs/heads/{branch}", job["candidate_commit"])
            b = STATE / "xfer" / f"{job['id']}-candidate.bundle"
            git(mirror, "bundle", "create", "-q", str(b), f"refs/heads/{branch}", f"^{job['base_commit']}")
            os.chmod(b, 0o640)
            if not project["publish"]["local_branch"]:
                self.upd(job["id"], publication=json.dumps({"done": True, "patch": str(final), "branch": None}))
                return
            rid = self.request("publish", job["id"], {"project": project["id"], "branch": branch, "sha": job["candidate_commit"],
                                                      "tree": tree, "bundle": str(b), "title": job["title"],
                                                      "pr_body": self.pr_body(job)})
            self.set_meta(job, publish_req=rid)
            self.ev(job["id"], "publish", f"publish request {rid} for {branch}")
            return
        res, row = self.request_result(m["publish_req"])
        if res is None:
            if now() - row["created_at"] > 600:
                raise Blocked("blocked_environment", "publisher did not answer within 10 min")
            return
        if row["status"] != "done":
            raise Blocked("blocked_environment", f"publication failed (candidate kept as patch): {res.get('error')}")
        res.update(done=True, patch=str(final), repo=project["source"]["path"])
        self.upd(job["id"], publication=json.dumps(res))
        self.ev(job["id"], "publish", f"published {branch}: local={res.get('local')} remote={res.get('remote')}"
                                      + (f" pr={res['draft_pr']}" if res.get("draft_pr") else ""))
        self.finish(job["id"], "ready_for_review", "required checks pass on the frozen candidate")

    def killed_by_infra(self, job, st, exit_code, oom):
        """A phase container that ended by signal (143/137) while nobody (timeout, cancel, quota
        pause) asked for it, and whose stop time is after dockerd last started, was stopped by a
        docker/host restart."""
        if oom or job["kill_reason"] or job["cancel_requested"] or exit_code not in (137, 143):
            return False
        if self.conn.execute("SELECT 1 FROM gateway_blocks WHERE attempt_id=? AND kind='quota'",
                             (job["current_attempt"],)).fetchone():
            return False
        try:
            started = self.dockerd_started()
            fin = st.get("FinishedAt") or ""
            finished = datetime.datetime.fromisoformat(fin[:26].rstrip("Z") + "+00:00").timestamp() if fin else 0
        except Exception:
            return False
        # The container stopped within the minute before dockerd's current start = shutdown of the
        # previous daemon instance.
        return started is not None and 0 <= started - finished <= 120

    def dockerd_started(self):
        r = subprocess.run(["systemctl", "show", "docker.service", "-p", "ActiveEnterTimestamp", "--value",
                            "--timestamp=unix"], capture_output=True, text=True, timeout=10)
        v = r.stdout.strip().lstrip("@")
        return float(v) if v.replace(".", "", 1).isdigit() else None

    def recover_missing(self, job, project):
        """The phase container is gone (host reboot, docker restart, manual removal)."""
        lim = project["limits"]
        aid = job["current_attempt"]
        if aid:
            self.end_attempt(aid, "lost", note="container disappeared")
        self.upd(job["id"], phase_container=None)
        if job["infra_retries"] >= lim["infra_retries"] or now() > (job["deadline"] or 0) - 120:
            raise Blocked("interrupted", f"{job['step']} container lost and no infrastructure retry remains")
        self.upd(job["id"], infra_retries=job["infra_retries"] + 1)
        job = db.job(self.conn, job["id"])
        self.ev(job["id"], "recover", f"relaunching {job['step']} (infrastructure retry {job['infra_retries']})")
        if job["step"] == "baseline":
            self.launch_checks(job, project, "baseline", job["base_commit"])
        elif job["step"] == "validate":
            self.launch_checks(job, project, f"validate-r{job['round']}", job["candidate_commit"])
        elif job["step"] == "worker":
            note = ("\n\nNOTE: your previous run of this round was interrupted by an infrastructure restart. "
                    "The working tree still contains your earlier edits; inspect `git status`/`git diff` and continue.")
            prev = (self.jdir(job["id"]) / "in-worker" / f"r{job['round']}" / "prompt.md").read_text()
            self.launch_worker(job, project, prev + note, resume=True)

    # ---------------------------------------------------------------- subscription quota pause
    def pause_for_quota(self, job, reason):
        """Worker stopped because the subscription window is near its limit: keep the checkout and
        session, stop the clock, and resume automatically once the window resets."""
        self.revoke_tokens(job["id"])
        remaining = max(600.0, (job["deadline"] or now()) - now())
        m = db.meta(job)
        m.update(paused_at=now(), paused_remaining=remaining, pause_reason=reason,
                 pauses=m.get("pauses", 0) + 1, pause_started=m.get("pause_started") or now())
        self.upd(job["id"], state="waiting_quota", step="worker", current_attempt=None, phase_container=None,
                 phase_deadline=None, meta=json.dumps(m))
        self.ev(job["id"], "pause", f"paused for subscription quota ({reason}); {remaining / 60:.0f} min of task time kept")
        if m["pauses"] == 1:
            self.notify(job["id"], f"⏸ **proctor {job['id']}** paused: {reason}. Resumes automatically when the window resets.")

    def step_waiting(self, job):
        m = db.meta(job)
        max_wait = self.settings.get("quota", {}).get("max_wait_hours", 24) * 3600
        if now() - m.get("pause_started", now()) > max_wait:
            raise Blocked("blocked_budget", f"subscription quota did not recover within {max_wait / 3600:.0f} h: {m.get('pause_reason')}")
        q = quota_block_reason(self.conn, self.settings, provider_for(job["model"]))
        if q:
            return False
        left, parts = remaining_allowance(self.conn, self.settings, job["id"])
        if left <= 0.05:
            raise Blocked("blocked_budget", f"allowance exhausted: {parts}")
        project = load_project(job["project"])
        self.upd(job["id"], deadline=now() + m["paused_remaining"])
        job = db.job(self.conn, job["id"])
        self.ev(job["id"], "resume", "subscription quota available again; resuming the worker")
        prev = (self.jdir(job["id"]) / "in-worker" / f"r{job['round']}" / "prompt.md").read_text()
        note = ("\n\nNOTE: your previous run of this round was paused because the model quota ran low. The working "
                "tree still contains your earlier edits; inspect `git status`/`git diff` and continue from there.")
        self.launch_worker(job, project, prev if "was paused because" in prev else prev + note, resume=True)
        return True

    # ---------------------------------------------------------------- cancellation
    def handle_cancel(self, job):
        self.ev(job["id"], "cancel", "cancellation requested")
        if job["state"] == "queued":
            self.upd(job["id"], state="cancelled", reason="cancelled before start", finished_at=now())
            self.notify(job["id"], f"⏹ **proctor {job['id']}** cancelled before start")
            return
        if job["phase_container"]:
            docker("kill", job["phase_container"], timeout=60)
        self.finish(job["id"], "cancelled", "cancelled by user")

    # ---------------------------------------------------------------- startup recovery
    def recover(self):
        known = {r["phase_container"] for r in self.conn.execute(
            f"SELECT phase_container FROM jobs WHERE state IN ({','.join('?' * len(ACTIVE))})", ACTIVE) if r["phase_container"]}
        r = docker("ps", "-a", "--filter", "label=proctor.job", "--format", "{{.Names}}")
        for name in r.stdout.split():
            if name not in known:
                docker("rm", "-f", name)
                log.warning("removed orphan container %s", name)
                db.event(self.conn, None, "recover", f"removed orphan container {name}")
        for j in self.conn.execute(f"SELECT * FROM jobs WHERE state IN ({','.join('?' * len(ACTIVE))})", ACTIVE).fetchall():
            c = inspect(j["phase_container"]) if j["phase_container"] else None
            what = "running, reattached" if c and c["state"].get("Running") else ("finished while down" if c else "no container")
            self.ev(j["id"], "recover", f"supervisor restart: {j['state']}/{j['step']} — {what}")

    # ---------------------------------------------------------------- main loop
    def tick(self):
        self.settings = load_settings()
        for j in self.conn.execute("SELECT * FROM jobs WHERE cancel_requested=1 AND state NOT IN (%s)" %
                                   ",".join("?" * len(TERMINAL)), TERMINAL).fetchall():
            self.guard(j, self.handle_cancel)
        active = self.conn.execute(f"SELECT * FROM jobs WHERE state IN ({','.join('?' * len(ACTIVE))}) ORDER BY created_at",
                                   ACTIVE).fetchall()
        for j in active:
            self.guard(j, self.advance)
        if not active:
            # A job paused on one provider's quota resumes first; while it waits, queued jobs on a
            # provider that is not blocked may run (still one active worker at a time).
            blocked = set()
            for w in self.conn.execute("SELECT * FROM jobs WHERE state='waiting_quota' AND cancel_requested=0 "
                                       "ORDER BY created_at").fetchall():
                prov = provider_for(w["model"])
                if prov in blocked:
                    continue
                self.guard(w, self.step_waiting)
                if db.job(self.conn, w["id"])["state"] != "waiting_quota":
                    return
                blocked.add(prov)
            for q in self.conn.execute("SELECT * FROM jobs WHERE state='queued' ORDER BY created_at").fetchall():
                if q["harness"] != "mock" and provider_for(q["model"]) in blocked:
                    continue
                self.guard(q, self.start_job)
                return

    def guard(self, job, fn):
        try:
            fn(job)
        except Blocked as b:
            self.finish(job["id"], b.state, b.reason)
        except ProjectError as e:
            self.finish(job["id"], "blocked_environment", f"project registry: {e}")
        except Exception as e:
            log.exception("job %s", job["id"])
            j = db.job(self.conn, job["id"])
            n = db.meta(j).get("internal_errors", 0) + 1
            self.set_meta(j, internal_errors=n, last_error=str(e)[:500])
            self.ev(job["id"], "error", f"supervisor error ({n}/3): {e}")
            if n >= 3:
                self.finish(job["id"], "failed", f"supervisor error: {e}")

    def advance(self, job):
        project = load_project(job["project"])
        if job["step"] == "sync":
            return self.step_sync(job, project)
        if job["step"] == "publish":
            return self.step_publish(job, project)
        if job["phase_container"] or job["step"] in ("baseline", "worker", "validate"):
            return self.step_container(job, project)

    def run(self):
        self.recover()
        while not STOP:
            try:
                self.tick()
            except Exception:
                log.exception("tick")
            for _ in range(4):
                if STOP:
                    break
                time.sleep(0.5)
        log.info("supervisor stopping; running containers are left in place for reattachment")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    os.umask(0o002)
    lockf = open(STATE / "run" / "supervisor.lock", "w")
    try:
        fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.error("another supervisor holds the lock; exiting")
        sys.exit(75)

    def stop(*_):
        global STOP
        STOP = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    Supervisor().run()


if __name__ == "__main__":
    main()

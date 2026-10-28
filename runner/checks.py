"""Run registered checks inside the runner. Usage: checks.py checks.json OUTDIR COMMIT
Writes OUTDIR/checks.json: {"commit", "prepare": [...], "checks": [{name, command, exit, seconds, status, log}]}"""
import json
import os
import signal
import subprocess
import sys
import time

spec = json.load(open(sys.argv[1]))
out = sys.argv[2]
commit = sys.argv[3]
res = {"commit": commit, "prepare": [], "checks": []}


def run(cmd, timeout, logf):
    t0 = time.time()
    with open(logf, "wb") as fh:
        p = subprocess.Popen(["bash", "-c", cmd], cwd="/work", stdout=fh, stderr=subprocess.STDOUT,
                             start_new_session=True)
        try:
            rc = p.wait(timeout=timeout)
            status = "pass" if rc == 0 else "fail"
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait()
            rc, status = 124, "timeout"
    if rc in (126, 127):
        status = "error"
    if rc in (137, -9) and status != "timeout":
        status = "oom"
    return rc, round(time.time() - t0, 2), status


ok = True
for i, step in enumerate(spec.get("prepare", [])):
    logf = f"{out}/logs/prepare-{i}.log"
    rc, sec, st = run(step, 600, logf)
    res["prepare"].append({"command": step, "exit": rc, "seconds": sec, "status": st, "log": os.path.basename(logf)})
    if rc != 0:
        ok = False
        break
if ok:
    for c in spec["checks"]:
        logf = f"{out}/logs/{c['name']}.log"
        rc, sec, st = run(c["command"], c.get("timeout", 600), logf)
        res["checks"].append({"name": c["name"], "command": c["command"], "required": c.get("required", True),
                              "exit": rc, "seconds": sec, "status": st, "log": f"logs/{c['name']}.log"})
try:
    res["memory_peak"] = int(open("/sys/fs/cgroup/memory.peak").read())
except Exception:
    pass
res["prepare_ok"] = ok
json.dump(res, open(f"{out}/checks.json", "w"), indent=1)

"""Shared paths, settings, project registry and small helpers (stdlib only)."""
from __future__ import annotations

import fnmatch
import json
import os
import re
import secrets
import time
import tomllib
from pathlib import Path

ETC = Path(os.environ.get("PROCTOR_ETC", "/etc/proctor"))
STATE = Path(os.environ.get("PROCTOR_STATE", "/var/lib/proctor"))
APP = Path(__file__).resolve().parent
SOCKET = Path(os.environ.get("PROCTOR_SOCKET", "/run/proctor/api.sock"))

# Visible job states (design doc vocabulary).
ACTIVE = ("preparing", "implementing", "validating", "repairing", "publishing")
TERMINAL = ("ready_for_review", "needs_input", "blocked_environment", "blocked_budget",
            "failed", "cancelled", "interrupted", "rejected")
WORKER_STATES = ("implementing", "repairing")

PROJECT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,40}$")
JOB_ID_RE = re.compile(r"^j[0-9]{4}-[0-9a-f]{6}$")


def now() -> float:
    return time.time()


def iso(t: float | None = None) -> str:
    if t is None:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))


def new_job_id() -> str:
    return "j" + time.strftime("%m%d") + "-" + secrets.token_hex(3)


# Defaults for every settings.toml table the services read, so a minimal file is enough to start.
# The gateway values must match bin/proctor-firewall, which reads them via `proctor/common.py gateway-env`.
SETTINGS_DEFAULTS = {
    "timezone": "UTC",
    "budget": {"per_task_usd": 0, "per_day_usd": 0, "per_month_usd": 0},
    "prices": {},
    "gateway": {"bind": "172.31.250.1", "port": 18780, "network": "proctor-gw",
                "subnet": "172.31.250.0/24", "interface": "proctor-gw0"},
    "runner": {"uid": 10001, "min_free_disk_gb": 5},
    "harness": {"claude_binary": "", "codex_binary": ""},
    # env: ANTHROPIC_API_KEY / OPENAI_API_KEY from the broker's environment (/etc/proctor/credentials.env).
    # hermes: subscription OAuth credentials, quota and notifications through a Hermes Agent install.
    "credentials": {"anthropic": "env", "openai": "env"},
    "hermes": {"agent_dir": "/usr/local/lib/hermes-agent", "bin": "/usr/local/bin/hermes"},
    # method: none | webhook (POST JSON {"text", "content"} to url) | hermes (`hermes send -t target`)
    "notify": {"method": "none", "url": "", "target": ""},
}
CREDENTIAL_SOURCES = ("env", "hermes")
NOTIFY_METHODS = ("none", "webhook", "hermes")


def _fill(d: dict, defaults: dict) -> dict:
    for k, v in defaults.items():
        if isinstance(v, dict):
            if not isinstance(d.get(k, {}), dict):
                raise ValueError(f"settings.toml: [{k}] must be a table")
            _fill(d.setdefault(k, {}), v)
        else:
            d.setdefault(k, v)
    return d


def load_settings() -> dict:
    with open(ETC / "settings.toml", "rb") as fh:
        raw = tomllib.load(fh)
    # Older files set only [notify].target, which always meant a Hermes delivery target.
    n = raw.get("notify")
    if isinstance(n, dict) and n.get("target") and "method" not in n:
        n["method"] = "hermes"
    s = _fill(raw, SETTINGS_DEFAULTS)
    for k in ("anthropic", "openai"):
        if s["credentials"][k] not in CREDENTIAL_SOURCES:
            raise ValueError(f"settings.toml: [credentials].{k} must be one of {CREDENTIAL_SOURCES}")
    if s["notify"]["method"] not in NOTIFY_METHODS:
        raise ValueError(f"settings.toml: [notify].method must be one of {NOTIFY_METHODS}")
    if s["notify"]["method"] == "webhook" and not re.match(r"^https?://", s["notify"]["url"]):
        raise ValueError("settings.toml: [notify].url must be an http(s) URL for method = \"webhook\"")
    return s


class ProjectError(ValueError):
    pass


def load_project(pid: str) -> dict:
    if not PROJECT_ID_RE.match(pid or ""):
        raise ProjectError(f"invalid project id {pid!r}")
    d = ETC / "projects" / pid
    f = d / "project.toml"
    if not f.is_file():
        raise ProjectError(f"project {pid!r} is not registered")
    with open(f, "rb") as fh:
        p = tomllib.load(fh)
    if p.get("id") != pid:
        raise ProjectError(f"{f}: id must be {pid!r}")
    allowed = {"id", "name", "enabled", "base_branch", "source", "harness", "limits", "runner", "publish",
               "protected_paths", "prepare", "instructions", "checks", "data", "orchestrator", "roles", "roles_common"}
    unknown = set(p) - allowed
    if unknown:
        raise ProjectError(f"{f}: unknown top-level keys {sorted(unknown)}")
    misplaced = {"protected_paths", "prepare", "instructions", "checks"}
    for tbl in ("source", "harness", "limits", "runner", "publish", "data", "orchestrator"):
        bad = misplaced & set(p.get(tbl, {}))
        if bad:
            raise ProjectError(f"{f}: {sorted(bad)} ended up inside [{tbl}]; move them above the first [table]")
    src = p.get("source") or {}
    if not src.get("path") or not src.get("owner") or src.get("owner") == "root":
        raise ProjectError(f"{f}: [source] needs path and a non-root owner")
    p["_dir"] = str(d)
    p.setdefault("enabled", True)
    p.setdefault("base_branch", "main")
    p.setdefault("checks", [])
    p.setdefault("prepare", [])
    p.setdefault("protected_paths", [])
    p.setdefault("instructions", "")
    p.setdefault("runner", {})
    p.setdefault("harness", {})
    p.setdefault("limits", {})
    p.setdefault("publish", {})
    p["harness"].setdefault("default", "claude-code")
    p["harness"].setdefault("allowed", [p["harness"]["default"]])
    p["harness"].setdefault("models", [])
    lim = p["limits"]
    lim.setdefault("task_minutes", 45)
    lim.setdefault("worker_minutes", 30)
    lim.setdefault("repair_rounds", 2)
    lim.setdefault("infra_retries", 1)
    lim.setdefault("max_turns", 120)
    lim.setdefault("max_patch_kb", 512)
    r = p["runner"]
    r.setdefault("memory", "2g")
    r.setdefault("cpus", 2.0)
    r.setdefault("pids", 512)
    r.setdefault("tmp_size", "512m")
    r.setdefault("env", {})
    r.setdefault("context_files", [])
    pub = p["publish"]
    pub.setdefault("local_branch", True)
    pub.setdefault("remote_push", False)
    pub.setdefault("draft_pr", False)
    pub.setdefault("remote", "origin")
    for c in p["checks"]:
        if not re.match(r"^[a-z0-9][a-z0-9_-]{0,30}$", c.get("name", "")):
            raise ProjectError(f"bad check name {c.get('name')!r}")
        c.setdefault("timeout", 600)
        c.setdefault("required", True)
        c.setdefault("data", False)
    # [data]: disposable copies of project data for workers / data checks (see docs/orchestrator-spec.md).
    data = p.setdefault("data", {})
    data.setdefault("copy", [])
    data.setdefault("mount_ro", [])
    data.setdefault("lock", None)
    data.setdefault("max_mb", 1024)
    for key in ("copy", "mount_ro"):
        if not isinstance(data[key], list) or not all(isinstance(x, str) for x in data[key]):
            raise ProjectError(f"{f}: [data].{key} must be a list of relative paths")
    for rel in data["copy"] + data["mount_ro"] + ([data["lock"]] if data["lock"] else []):
        if not _safe_rel(rel):
            raise ProjectError(f"{f}: [data] path {rel!r} must be relative without '..'")
    # [orchestrator]: autonomous planning/review on top of proctor; off unless the project opts in.
    o = p.setdefault("orchestrator", {})
    o.setdefault("enabled", False)
    o.setdefault("brief", "orchestrator.md")
    o.setdefault("task_model", "")
    o.setdefault("max_tasks_per_day", 8)
    o.setdefault("max_open_reviews", 3)
    o.setdefault("night", "23-05")
    o.setdefault("day_gap_hours", 3)
    o.setdefault("merge", "none")
    o.setdefault("push", False)
    o.setdefault("approval_paths", [])
    o.setdefault("approval_checks", [])   # check names whose non-pass result needs owner approval to merge
    o.setdefault("post_merge", [])
    o.setdefault("post_merge_path", [])
    if o["merge"] not in ("none", "audit"):
        raise ProjectError(f"{f}: [orchestrator].merge must be 'none' or 'audit'")
    if o["push"] and o["merge"] != "audit":
        raise ProjectError(f"{f}: [orchestrator].push needs merge = 'audit'")
    if not re.match(r"^\d{1,2}-\d{1,2}$", str(o["night"])):
        raise ProjectError(f"{f}: [orchestrator].night must look like '23-05'")
    for step in o["post_merge"]:
        if not isinstance(step, dict) or not isinstance(step.get("run"), str):
            raise ProjectError(f"{f}: [orchestrator].post_merge entries need a 'run' string")
        step.setdefault("when", [])
        step.setdefault("timeout", 900)
    if "/" in o["brief"] or o["brief"].startswith("."):
        raise ProjectError(f"{f}: [orchestrator].brief must be a file name in the registry dir")
    o.setdefault("review", None)   # None = follow [review].enabled in settings.toml; true/false overrides
    # Research lane: up to N tasks per night window with this role, counted apart from the main daily cap/spacing.
    o.setdefault("research_tasks_per_night", 0)
    o.setdefault("research_role", "research")
    if not isinstance(o["research_tasks_per_night"], int) or o["research_tasks_per_night"] < 0:
        raise ProjectError(f"{f}: [orchestrator].research_tasks_per_night must be an integer >= 0")
    if o["review"] is not None and not isinstance(o["review"], bool):
        raise ProjectError(f"{f}: [orchestrator].review must be true or false")
    _load_roles(p, f, d)
    return p


ROLE_RE = re.compile(r"^[a-z][a-z0-9_-]{0,30}$")


def _load_roles(p: dict, f, d: Path) -> None:
    """Validate [roles.<name>] and [roles_common]; fills defaults. Roles restrict which paths a job may change."""
    roles = p.setdefault("roles", {})
    if not isinstance(roles, dict):
        raise ProjectError(f"{f}: [roles] must be a table of [roles.<name>]")
    common_t = p.setdefault("roles_common", {})
    if not isinstance(common_t, dict) or set(common_t) - {"shared_paths"}:
        raise ProjectError(f"{f}: [roles_common] only supports shared_paths")
    shared = common_t.setdefault("shared_paths", [])
    if not isinstance(shared, list) or not all(isinstance(x, str) and x for x in shared):
        raise ProjectError(f"{f}: [roles_common].shared_paths must be a list of glob strings")
    check_names = {c["name"] for c in p["checks"]}
    for name, r in roles.items():
        if not ROLE_RE.match(name):
            raise ProjectError(f"{f}: bad role name {name!r} (use [a-z][a-z0-9_-]{{0,30}})")
        if not isinstance(r, dict):
            raise ProjectError(f"{f}: [roles.{name}] must be a table")
        unknown = set(r) - {"brief", "paths", "model", "fallback_model", "checks"}
        if unknown:
            raise ProjectError(f"{f}: [roles.{name}] unknown keys {sorted(unknown)}")
        paths = r.get("paths")
        if not isinstance(paths, list) or not paths or not all(isinstance(x, str) and x for x in paths):
            raise ProjectError(f"{f}: [roles.{name}].paths must be a non-empty list of glob strings")
        for g in paths + shared:
            if g.startswith("/") or ".." in Path(g).parts:
                raise ProjectError(f"{f}: [roles.{name}] path glob {g!r} must be relative without '..'")
        brief = r.get("brief", "")
        if not isinstance(brief, str) or not brief or brief.startswith("/") or ".." in Path(brief).parts:
            raise ProjectError(f"{f}: [roles.{name}].brief must be a relative file path inside the registry dir")
        bf = (d / brief)
        if not bf.is_file():
            raise ProjectError(f"{f}: [roles.{name}].brief {brief!r} not found in {d}")
        r["_brief_path"] = str(bf)
        r.setdefault("model", "")
        if not isinstance(r["model"], str):
            raise ProjectError(f"{f}: [roles.{name}].model must be a string")
        r.setdefault("fallback_model", "")
        if r["fallback_model"] and not model_allowed(p, load_settings(), r["fallback_model"]):
            raise ProjectError(f"{f}: [roles.{name}].fallback_model {r['fallback_model']!r} is not approved for {p['id']} or has no price entry")
        r.setdefault("checks", [])
        if not isinstance(r["checks"], list) or not all(isinstance(x, str) for x in r["checks"]):
            raise ProjectError(f"{f}: [roles.{name}].checks must be a list of check names")
        for cn in r["checks"]:
            if cn not in check_names:
                raise ProjectError(f"{f}: [roles.{name}].checks names unknown check {cn!r}")
        if r["model"]:
            # Imported lazily: settings.toml is only needed to price/allow the role's default model.
            if not model_allowed(p, load_settings(), r["model"]):
                raise ProjectError(f"{f}: [roles.{name}].model {r['model']!r} is not approved for {p['id']} or has no price entry")


def db_meta_role(job) -> str:
    """The role stored in a job row's meta JSON ('' when none)."""
    try:
        return json.loads(job["meta"] or "{}").get("role") or ""
    except Exception:
        return ""


def review_enabled(project: dict, settings: dict) -> bool:
    """Cross-model review gates orchestrator merges: global [review].enabled, overridable per project."""
    ov = (project.get("orchestrator") or {}).get("review")
    return bool(ov) if ov is not None else bool((settings.get("review") or {}).get("enabled", False))


def role_paths(project: dict, role: str) -> list[str]:
    """Globs a job with this role may change: the role's paths plus the project-wide shared_paths."""
    r = project["roles"][role]
    return list(r["paths"]) + list(project["roles_common"]["shared_paths"])


def role_violations(project: dict, role: str, files: list[str]) -> list[str]:
    allowed = role_paths(project, role)
    return [x for x in files if not any(fnmatch.fnmatch(x, g) for g in allowed)]


def role_model(project: dict, role: str, blocked) -> str:
    """The role's model, or its fallback_model when blocked(provider) says the primary's provider is quota-blocked."""
    r = project["roles"][role]
    m, fb = r.get("model") or "", r.get("fallback_model") or ""
    if m and fb and blocked(provider_for(m)) and not blocked(provider_for(fb)):
        return fb
    return m


def role_brief(project: dict, role: str) -> str:
    return (read_small(Path(project["roles"][role]["_brief_path"]), 64_000) or "").strip()


def _safe_rel(rel: str) -> bool:
    return bool(rel) and not rel.startswith("/") and ".." not in Path(rel).parts


def list_projects() -> list[str]:
    d = ETC / "projects"
    if not d.is_dir():
        return []
    return sorted(x.name for x in d.iterdir() if (x / "project.toml").is_file())


def path_protected(path: str, globs: list[str]) -> bool:
    return any(fnmatch.fnmatch(path, g) for g in globs)


def price_for(settings: dict, model: str) -> dict | None:
    prices = settings.get("prices", {})
    if model in prices:
        return prices[model]
    for pat, p in prices.items():
        if fnmatch.fnmatch(model, pat):
            return p
    return None


# Provider of a model id. Each provider has its own subscription-quota row (quota.id).
QUOTA_ROW = {"anthropic": 1, "openai-codex": 2}
PROVIDER_LABEL = {"anthropic": "Claude", "openai-codex": "ChatGPT"}


def provider_for(model: str) -> str:
    m = (model or "").lower()
    return "openai-codex" if m.startswith(("gpt-", "codex-", "o3", "o4")) else "anthropic"


def model_allowed(project: dict, settings: dict, model: str) -> bool:
    pats = project["harness"].get("models") or []
    return bool(model) and any(fnmatch.fnmatch(model, p) for p in pats) and price_for(settings, model) is not None


def usage_cost(price: dict, u: dict) -> float:
    m = 1e-6
    return (u.get("input", 0) * price["input"] + u.get("output", 0) * price["output"]
            + u.get("cache_read", 0) * price["cache_read"] + u.get("cache_write", 0) * price["cache_write"]) * m


def dumps(o) -> str:
    return json.dumps(o, sort_keys=True, default=str)


def tail_text(path: Path, max_bytes: int = 4000) -> str:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        return ""
    with os.fdopen(fd, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        fh.seek(max(0, size - max_bytes))
        return fh.read().decode("utf-8", "replace")


def read_small(path: Path, max_bytes: int = 1_000_000) -> str | None:
    """Read a possibly-untrusted file without following symlinks."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        return None
    with os.fdopen(fd, "rb") as fh:
        return fh.read(max_bytes).decode("utf-8", "replace")


def gateway_env(settings: dict) -> str:
    """Shell assignments for bin/proctor-firewall, so the firewall and the gateway share one source of truth."""
    import ipaddress
    import shlex
    gw = settings["gateway"]
    net = ipaddress.ip_network(gw["subnet"])
    if ipaddress.ip_address(gw["bind"]) not in net:
        raise ValueError(f"[gateway].bind {gw['bind']} is outside [gateway].subnet {gw['subnet']}")
    if not re.match(r"^[a-z0-9][a-z0-9_.-]{0,14}$", gw["interface"]):
        raise ValueError(f"[gateway].interface {gw['interface']!r} is not a valid interface name")
    vals = {"NET": gw["network"], "IF": gw["interface"], "SUBNET": str(net), "GW": gw["bind"], "PORT": int(gw["port"])}
    return "\n".join(f"{k}={shlex.quote(str(v))}" for k, v in vals.items())


if __name__ == "__main__":
    import sys
    if sys.argv[1:] != ["gateway-env"]:
        sys.exit("usage: common.py gateway-env")
    print(gateway_env(load_settings()))

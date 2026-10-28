"""Harness adapters. Each adapter knows how to configure the in-container worker and how to
normalise its event stream. The supervisor never trusts an adapter's claims about tests."""
from __future__ import annotations

import json
from pathlib import Path

from .common import db_meta_role, read_small, role_brief, role_paths

COMPLETION_CONTRACT = """\
Implement only the assigned task in the provided workspace. Preserve the behavior outside the
acceptance criteria. Diagnose failures before editing. Add meaningful regression coverage when
needed. Do not weaken, skip or delete checks to obtain a pass. Report what changed, exact checks
performed, limitations, and any unresolved questions. Stop when acceptance criteria are met or the
task is blocked. Save a checkpoint before switching strategies."""

REPORT_SPEC = """\
Before you finish, write a JSON report to the file named in $SWE_REPORT (outside the repository):
{"status": "done" | "blocked" | "needs_input",
 "summary": "<what changed, 1-5 sentences>",
 "checks_run": ["<exact command> -> <result>", ...],
 "limitations": ["..."], "questions": ["..."]}
Use "needs_input" when the requirements are ambiguous or contradictory, "blocked" when the
environment prevents the work. Checkpoints: append one line per milestone/strategy change to
the file named in $SWE_CHECKPOINT. Do not commit; leave changes in the working tree.
The supervisor re-runs every registered check itself on a fresh checkout; your claims are not
used as evidence."""


def task_prompt(job, project, baseline: list[dict]) -> str:
    acc = json.loads(job["acceptance"])
    checks = "\n".join(f"- {c['name']}: `{c['command']}`" + ("" if c.get("required", True) else " (informational)")
                       for c in project["checks"])
    base_lines = "\n".join(f"- {b['name']}: {b['status']} (exit {b['exit']}, {b['seconds']}s)" for b in baseline) or "- (none)"
    d = project.get("data") or {}
    data_note = ""
    if d.get("copy") or d.get("mount_ro"):
        parts = []
        if d.get("copy"):
            parts.append("copied into /work as writable throwaway snapshots, discarded with the job and never part of "
                         "your change: " + ", ".join(f"`{x}`" for x in d["copy"]))
        if d.get("mount_ro"):
            parts.append("mounted read-only: " + ", ".join(f"`{x}`" for x in d["mount_ro"]))
        data_note = "\nProject data (" + "; ".join(parts) + ").\n"
    role_note = ""
    rname = db_meta_role(job)
    if rname and rname in project.get("roles", {}):
        role_note = (f"## Your role: {rname}\n{role_brief(project, rname)}\n\n"
                     f"You may change ONLY files matching these paths: {', '.join('`' + g + '`' for g in role_paths(project, rname))}. "
                     "The supervisor rejects any change outside them; if the task needs other files, say so in your report "
                     "(needs_input) instead of editing them.\n\n")
    return f"""# Task {job['id']}: {job['title']}

## Goal
{job['goal']}

## Acceptance criteria
{chr(10).join('- ' + a for a in acc)}

## Project
{project.get('name', project['id'])}. Base branch `{job['base_branch']}`, base commit `{job['base_commit']}`.
You are in a fresh, isolated checkout at /work (branch `swe/{job['id']}`). There is no network
access except the model API; dependencies are preinstalled in the image. Do not try to install
packages or reach the internet.

{role_note}{project.get('instructions', '').strip()}
{data_note}
## Registered checks (run by the supervisor on a fresh checkout of your exact change)
{checks}

## Baseline at the base commit (before your change)
{base_lines}

## Completion contract
{COMPLETION_CONTRACT}

## Report
{REPORT_SPEC}
"""


def repair_prompt(job, failures: list[dict], round_no: int, max_rounds: int) -> str:
    parts = [f"# Repair round {round_no} of {max_rounds} for task {job['id']}",
             "The supervisor validated your change on a fresh checkout and these required checks failed.",
             "Diagnose the cause from the evidence below, fix the change (not the checks), then write a new report.", ""]
    for f in failures:
        parts += [f"## {f['name']}: {f['status']} (exit {f['exit']})", f"Command: `{f['command']}`",
                  "```", f["tail"].strip()[-3500:], "```", ""]
    return "\n".join(parts)


def role_repair_prompt(job, role: str, allowed: list[str], bad: list[str], round_no: int, max_rounds: int) -> str:
    lines = "\n".join(f"- {b}" for b in bad[:50])
    return (f"# Repair round {round_no} of {max_rounds} for task {job['id']}\n"
            f"Your change touches files outside the {role} role's paths. The supervisor rejects it as it is.\n\n"
            f"Files outside the role's paths:\n{lines}\n\n"
            f"Allowed paths: {', '.join(allowed)}\n\n"
            "Revert (restore to the base version, or delete if you created them) every file outside the allowed paths, or "
            "put that part of the change in a separate task by reporting `needs_input`. Keep the in-scope part of the work, "
            "then write a new report.\n")


class Adapter:
    name = "base"
    provider = "anthropic"  # whose subscription the gateway bills
    network = True          # needs the model gateway
    resumable = False

    def env(self, job, attempt, token, settings) -> dict:
        return {}

    def parse(self, out_dir: Path) -> dict:
        return {}


class ClaudeCode(Adapter):
    """Claude Code CLI in print mode, stream-json events, pointed at the swe-pilot gateway."""
    name = "claude-code"
    resumable = True

    def env(self, job, attempt, token, settings):
        gw = settings["gateway"]
        return {
            "ANTHROPIC_BASE_URL": f"http://{gw['bind']}:{gw['port']}",
            "ANTHROPIC_AUTH_TOKEN": token,
            "DISABLE_AUTOUPDATER": "1",
            "DISABLE_TELEMETRY": "1",
            "DISABLE_ERROR_REPORTING": "1",
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            "CLAUDE_CODE_DISABLE_FEEDBACK_SURVEY": "1",
        }

    def parse(self, out_dir):
        res = {"session_id": None, "result": None, "is_error": None, "subtype": None,
               "reported_cost_usd": None, "num_turns": None, "events": 0, "tools": 0}
        text = read_small(out_dir / "worker.jsonl", 50_000_000) or ""
        for line in text.splitlines():
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            res["events"] += 1
            if ev.get("session_id"):
                res["session_id"] = ev["session_id"]
            if ev.get("type") == "assistant":
                for blk in (ev.get("message") or {}).get("content") or []:
                    if blk.get("type") == "tool_use":
                        res["tools"] += 1
            if ev.get("type") == "result":
                res.update(result=(ev.get("result") or "")[:4000], is_error=ev.get("is_error"),
                           subtype=ev.get("subtype"), reported_cost_usd=ev.get("total_cost_usd"),
                           num_turns=ev.get("num_turns"), usage=ev.get("usage"))
        return res


class Codex(Adapter):
    """OpenAI Codex CLI (`codex exec --json`) on the ChatGPT subscription, through the gateway.

    Codex is configured with a custom Responses-API provider whose base_url is the gateway and whose
    bearer token is the per-attempt token (env_key). The real ChatGPT OAuth token stays in the broker.
    Codex's own sandbox is bypassed: the container is the sandbox (non-root, caps dropped, no network
    except the gateway); its bubblewrap/landlock sandbox cannot run without privileges anyway."""
    name = "codex"
    provider = "openai-codex"
    resumable = True

    def env(self, job, attempt, token, settings):
        gw = settings["gateway"]
        return {"SWE_GATEWAY_URL": f"http://{gw['bind']}:{gw['port']}/v1", "SWE_GATEWAY_TOKEN": token,
                "CODEX_HOME": "/home/swe/.codex", "NO_COLOR": "1", "RUST_LOG": "warn",
                "SWE_REASONING": settings.get("harness", {}).get("codex_reasoning_effort", "medium")}

    def parse(self, out_dir):
        res = {"session_id": None, "result": None, "is_error": None, "subtype": None,
               "reported_cost_usd": None, "num_turns": 0, "events": 0, "tools": 0, "usage": None}
        text = read_small(out_dir / "worker.jsonl", 50_000_000) or ""
        last_msg, errors = None, []
        for line in text.splitlines():
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            res["events"] += 1
            t = ev.get("type")
            if t == "thread.started":
                res["session_id"] = ev.get("thread_id")
            elif t == "turn.completed":
                res["num_turns"] += 1
                res["usage"] = ev.get("usage")
            elif t == "turn.failed":
                errors.append(str((ev.get("error") or {}).get("message") or ev)[:500])
            elif t == "error":
                errors.append(str(ev.get("message") or ev)[:500])
            elif t == "item.completed":
                it = ev.get("item") or {}
                if it.get("type") == "agent_message":
                    last_msg = it.get("text")
                elif it.get("type") in ("command_execution", "file_change", "mcp_tool_call"):
                    res["tools"] += 1
        res["result"] = (last_msg or "")[:4000]
        res["is_error"] = bool(errors)
        res["subtype"] = "error" if errors and not last_msg else "success" if last_msg else "no_result"
        if errors:
            res["errors"] = errors[-3:]
        return res


class Mock(Adapter):
    """Deterministic scripted worker for pipeline verification. Makes no model calls."""
    name = "mock"
    network = False
    resumable = True

    def parse(self, out_dir):
        res = {"session_id": "mock", "result": None, "is_error": False, "subtype": "mock", "events": 0}
        for line in (read_small(out_dir / "worker.jsonl") or "").splitlines():
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            res["events"] += 1
            if ev.get("type") == "result":
                res["result"] = ev.get("result")
        return res


ADAPTERS = {a.name: a for a in (ClaudeCode(), Codex(), Mock())}


def default_harness_for(model: str, project: dict) -> str:
    """Pick the harness that serves a model's provider (first allowed one for the project)."""
    from .common import provider_for
    prov = provider_for(model)
    for name in project["harness"].get("allowed", []):
        if name in ADAPTERS and ADAPTERS[name].provider == prov:
            return name
    return project["harness"]["default"]

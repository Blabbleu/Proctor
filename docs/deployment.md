# Live deployment: operator guide

`install.sh` sets up the live engine on one host: service account, directories, application, CLI, configuration and systemd units. It is tested with `--dry-run` and static checks; a full install on a fresh host is still listed under "Not verified" in [verification.md](verification.md). Use a disposable dedicated Linux host before connecting a real project.

## Requirements

- Linux with Python 3.11+, Git, Docker Engine, systemd, `runuser`, and working IPv4/IPv6 iptables compatibility. The installer checks these first.
- A non-root account owning each source repository. The installer creates the `proctor` service account.
- Adequate disk headroom and container resources; example limits are configuration, not a performance benchmark.
- For real models, one of:
  - **API keys (default):** `ANTHROPIC_API_KEY` and/or `OPENAI_API_KEY`, used by the broker against the public Messages and Responses APIs.
  - **Subscriptions via Hermes:** Hermes Agent installed through its [official instructions](https://hermes-agent.nousresearch.com/docs/); install with `--hermes`. This is also the only source of subscription-quota readings.
- Operator-installed Claude Code and optionally Codex, from their official distributions. These binaries are not included. The installer stages a `claude` found on `PATH`; the mock harness needs neither.

Read [SECURITY.md](../SECURITY.md) first. Docker-group membership grants root-equivalent authority. Do not use a shared sensitive host as an untrusted code service.

## Filesystem contract

| Location | Purpose / ownership |
|---|---|
| `/opt/proctor` | reviewed application source, root-owned, not worker-writable |
| `/opt/proctor/harness` | staged harness binaries copied into the runner base image |
| `/etc/proctor/settings.toml` | trusted settings, root-owned and service-group-readable |
| `/etc/proctor/credentials.env` | API keys for the broker only, root `0600` |
| `/etc/proctor/projects/<id>` | trusted registry, image recipe, brief and optional roles |
| `/var/lib/proctor` | service-owned state, job artifacts, mirrors and build context |
| `/run/proctor` | runtime lock/socket area as used by the code |
| `/run/proctor-xfer` | broker transfer area |

`PROCTOR_ETC` and `PROCTOR_STATE` override configuration/state paths for offline development. They do not make the entire live service relocatable: unit files, helper commands, runtime transfer paths, and host integrations still have deployment assumptions.

## Install

```bash
sudo ./install.sh --dry-run    # every action, nothing changed
sudo ./install.sh              # install or upgrade, start, run `proctor doctor`
```

| Option | Effect |
|---|---|
| `--no-start` | install and enable the units without starting them |
| `--hermes` | `[credentials]` = `hermes` and a broker drop-in that runs under Hermes' interpreter |
| `--uninstall` | stop and remove units, application, CLI wrapper, firewall chain and runner network |
| `--uninstall --purge` | also delete `/etc/proctor`, `/var/lib/proctor` and the `proctor` account |

Re-running upgrades the application in place and restarts the services; it never overwrites `settings.toml`, `credentials.env` or project registrations. On a fresh install it picks the first gateway subnet that does not overlap an existing route; with an existing `settings.toml` an overlap stops the install instead.

## Configure

1. **Check health:** `sudo proctor doctor`. `BAD` lines fail; `WARN` lines only matter for real (non-mock) work.
2. **Register a project:** `sudo proctor project add <id> --path <repo> [--check name=command ...]`. It writes `project.toml`, a `Dockerfile` and a brief under `/etc/proctor/projects/<id>`. The owner defaults to the repository directory's owner and the base branch to its current branch. Install check dependencies in the `Dockerfile`: worker and check containers cannot download packages.
3. **Try the mock harness:** `sudo proctor submit <id> '<goal>' -a '<acceptance>' --harness mock`, then `proctor list` / `proctor status <job>`. Registrations default to `[publish].local_branch = true`, which leaves a reviewable branch in the source repository and pushes nothing.
4. **Enable a real model:** add keys to `credentials.env` and `sudo systemctl restart proctor-broker`. In `settings.toml`, set `[harness].claude_binary` (or `codex_binary`), a `[prices]` entry for each model and positive `[budget]` ceilings. Re-register with `--harness claude-code --model <id> --force`.
5. **Notifications (optional):** `[notify] method = "webhook"` with a Slack- or Discord-style incoming webhook `url`, or `method = "hermes"` with a Hermes `target`.

Leave `[orchestrator].enabled = false`, `merge = "none"`, `push = false` and `[publish].remote_push = false` during initial verification. The firewall script changes host firewall rules for the runner bridge only; review `bin/proctor-firewall` and keep an out-of-band recovery path. Do not edit production registry or firewall settings from inside a worker.

## Inspect the installed system

Run these only in the operator-configured environment; unlike the demo, they address real service state:

```bash
proctor projects
proctor doctor
proctor usage
proctor list
proctor orch status
```

A complete first integration test should submit a mock task against the disposable project and verify the candidate SHA and patch, actual supervisor check logs, attempt records, and refusal/recovery behavior. The example sets `[publish].local_branch = false`, so no final branch is published. To test local branch publication, deliberately enable that option for the disposable project and verify the final branch SHA. Do not infer this from the offline demo.

Use `proctor orch context <project>` to inspect a planner briefing. Do **not** use `proctor orch gate` as a read-only status command: it updates wake and decision markers.

## Enabling real models and autonomy

- Verify currently supported model IDs and provider terms; configure exact allow-lists and verified prices. The code's routing distinguishes Codex/OpenAI-style IDs from Anthropic-style IDs; arbitrary provider integrations are not implemented.
- Confirm quota observations and `require_fresh` behavior. A cached row can be stale even when a guard is enabled.
- Keep provider-side extra usage disabled if your account must not incur it. Estimated gateway budgets are not billing enforcement.
- Choose worker and reviewer models independently. Configure only the providers you intend to use; do not add an automatic paid fallback.
- Planner scheduling is a separate integration; see [planner-integration.md](planner-integration.md).
- Only after evidence checks pass should you opt in to automatic planning or audited fast-forward merging. Remote branch push, draft PR creation, and post-merge hooks are separate trust decisions.

## Recovery and backup

Back up trusted configuration, a consistent SQLite snapshot, repository refs/mirrors, and any artifacts you need to retain. Treat backups as sensitive. Test restore on a disposable host.

A supervisor restart is intended to reattach to surviving Docker containers; broker restarts affect the worker's model route. Cancellation attempts to preserve a patch but preservation is best-effort. A post-merge hook failure does not automatically roll back Git or a deployed service. Read event and check records before retrying or declaring recovery complete.

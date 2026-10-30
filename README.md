# Proctor

**A supervised execution layer for AI coding agents — they don't grade their own work.**

Turn a scoped task into an isolated worker run, validate the exact resulting commit independently, and retain an auditable review artifact before a human or policy-controlled merge.

Built from a self-hosted deployment; extracted into a public development edition without private project registries, credentials, runtime data, or bundled proprietary harness binaries. This is an early engineering project, not a hosted service or an independently audited sandbox.

## Why it exists

A coding agent saying “tests passed” is not deployment evidence. Proctor separates planning, implementation, validation, and repository publication so that a worker's report cannot substitute for supervisor-run checks.

The interesting work is in the control plane:

- **Durable execution:** SQLite jobs, attempts, events, checks, usage, requests, and notification outbox.
- **Frozen candidates:** export the worker tree into a trusted checkout; validate that candidate rather than trusting the worker's Git metadata.
- **Independent validation:** baseline and candidate checks in fresh offline containers, with bounded repair rounds.
- **Controlled model access:** per-attempt gateway tokens, model allow-lists, usage reservations, metering, and subscription-quota guards.
- **Explicit autonomy:** project-scoped task caps, review capacity, role path restrictions, merge policies, and owner approvals.
- **Recoverability:** container reattachment, stale-attempt rejection, infrastructure retry limits, and best-effort preservation of partial patches.

## Run the offline checks

No host setup or credentials needed: Linux, Python 3.11+, and Git are sufficient for the offline development path. No third-party Python runtime dependencies are required.

```bash
python3 -m unittest discover -s tests -v
python3 examples/control_plane_demo.py
python3 scripts/check_public_release.py
```

The demo exercises real task-intake and policy code against temporary local state. It **does not** run a coding model, start the Docker supervisor, prove firewall isolation, or demonstrate a successful end-to-end worker job. Its output is produced by execution rather than a canned transcript.

GitHub Actions runs the same offline checks on Python 3.11–3.13 for every push and pull request.

## Install on a host

On a dedicated Linux host with Docker, systemd and Python 3.11+:

```bash
git clone https://github.com/Blabbleu/Proctor.git && cd Proctor
sudo ./install.sh --dry-run        # see every change first
sudo ./install.sh                  # install, start the services, run `proctor doctor`

sudo proctor project add myproj --path /srv/myproj --check 'test=python3 -m unittest -q'
sudo proctor submit myproj 'Describe a small change' -a 'what done looks like' --harness mock
sudo proctor list
```

A fresh install runs the scripted mock harness with no credentials, picks a free gateway subnet, and stages an installed Claude Code binary if one is on `PATH`. For real models, add API keys to `/etc/proctor/credentials.env`, then set prices and spending ceilings in `/etc/proctor/settings.toml`. To use Claude/ChatGPT subscriptions through Hermes Agent instead of API keys, install with `--hermes`. Re-running `sudo ./install.sh` upgrades in place and keeps your configuration. `sudo ./install.sh --uninstall` removes everything except configuration and job history (`--purge` removes those too). See the [operator guide](docs/deployment.md).

## Using Proctor

**1. Register a project.** Proctor works on a local Git repository owned by a non-root user:

```bash
sudo proctor project add myproj --path /srv/myproj --check 'test=python3 -m pytest -q' --check 'lint=ruff check .'
```

Checks and workers run in containers **with no network access**, so the project's test dependencies must be installed at image build time. Add them to `/etc/proctor/projects/myproj/Dockerfile`, for example `RUN pip install --break-system-packages pytest ruff`. Proctor runs every check on the untouched base commit first. A command that is not found stops the job as `blocked_environment` before any agent runs. Any other baseline failure, including `python3 -m pytest` without pytest installed, is recorded as pre-existing and the job continues, so look at the baseline results in `proctor status` on your first job.

**2. Submit a task.** A task is a goal plus at least one acceptance criterion. A goal shorter than 15 characters, or a task with no `-a`, is parked as `needs_input` instead of run.

```bash
sudo proctor submit myproj 'Return 404 instead of 500 when a user id does not exist' \
    -a 'GET /users/<missing id> returns 404' -a 'existing tests still pass' --harness mock
```

`--harness mock` runs a scripted worker with no model calls. Pick a scenario with `--mock` to exercise each path: `good` (passes), `fixable` (fails once, then repairs), `bad` (exhausts repair rounds), `noop` (no change), `ask` (needs input), `slow` (for trying `cancel`).

**3. Follow it.**

| Command | Shows |
|---|---|
| `proctor list` | recent jobs, state, cost |
| `proctor status <job>` | state, base and candidate commits, supervisor-run check results, published branch |
| `proctor logs <job>` | event timeline (`--worker` for the agent transcript, `--check <name>` for a check log) |
| `proctor cancel <job>` | stop a running job; its partial patch is preserved when possible |
| `proctor retry <job>` | resubmit a failed, blocked or cancelled job on a fresh base |
| `proctor usage` | metered spend against the budget ceilings |
| `proctor doctor` | service, network, firewall, credential and project health |

**4. Review and merge.** A job that passes its checks ends in `ready_for_review`. The candidate is published as branch `proctor/<job-id>` in the project's repository, and the patch sits under the job's artifacts directory (see `proctor status`). Proctor does not merge or push by default. Review the branch and merge it yourself, as the repository owner:

```bash
git -C /srv/myproj diff main...proctor/<job-id>
git -C /srv/myproj merge --ff-only proctor/<job-id>
```

Automatic planning and audited fast-forward merges are opt-in per project. See [planner integration](docs/planner-integration.md).

## Using a real model

1. Add an API key to `/etc/proctor/credentials.env` (`ANTHROPIC_API_KEY=` or `OPENAI_API_KEY=`), then run `sudo systemctl restart proctor-broker`. Workers never see this key; they get a per-attempt gateway token.
2. In `/etc/proctor/settings.toml`, point `[harness]` at the harness binary, price the model, and set spending ceilings. The gateway refuses every model call while a ceiling is `0`, and it only allows priced models.

   ```toml
   [budget]
   per_task_usd = 2
   per_day_usd = 10
   per_month_usd = 100

   [prices]
   # USD per million tokens. Use the provider's current list prices.
   "claude-sonnet-5-5" = { input = <in>, output = <out>, cache_read = <read>, cache_write = <write> }

   [harness]
   claude_binary = "/opt/proctor/harness/claude-<version>"   # filled in by install.sh if claude was on PATH
   ```

3. Allow the model for the project, then submit without `--harness mock`. `--force` rewrites `project.toml`, so redo any hand edits:

   ```bash
   sudo proctor project add myproj --path /srv/myproj --check 'test=python3 -m pytest -q' \
       --harness claude-code --model claude-sonnet-5-5 --force
   sudo proctor doctor
   ```

Budget figures are gateway estimates at list price, not billing limits. Keep a spending limit with the provider as well.

## Troubleshooting

- Run `sudo proctor doctor` first: `BAD` lines are broken, `WARN` lines only matter for real models.
- Service logs: `journalctl -u proctor-broker -u proctor-supervisor -u proctor-firewall -e`.
- A job's `reason` field (`proctor status <job>`) explains `blocked_environment`, `blocked_budget` and `needs_input` outcomes, for example an image build failure or an exhausted ceiling.

## Architecture

```mermaid
flowchart TD
    Owner[Owner / CLI / planner] --> CLI[proctor CLI + orchestrator policies]
    CLI --> DB[(SQLite state and audit events)]
    DB <--> Supervisor[Trusted deterministic supervisor]
    Supervisor --> Worker[Non-root Docker coding worker]
    Worker --> Gateway[Trusted model credential gateway]
    Gateway --> Provider[Configured model provider]
    Gateway --> DB
    Worker --> Export[Export into trusted Git mirror]
    Export --> Check[Fresh offline candidate checks]
    Check --> Evidence[Candidate SHA + check logs + patch]
    Evidence --> Review[Owner / configured model review]
    Review --> Broker[Trusted repository broker]
    Broker --> Branch[Local review branch / optional publication]
```

The worker container is the untrusted execution boundary. The broker, supervisor, Docker daemon, project registry, and host are trusted. Docker access is root-equivalent; separate Unix service users do not establish a strong security boundary between those services.

[Architecture and trade-offs](docs/architecture.md) · [Threat model](SECURITY.md) · [Operator setup](docs/deployment.md) · [Planner integration](docs/planner-integration.md)

## Job lifecycle

```text
queued → preparing → baseline validation → implementing
    → export/freeze → candidate validation → publishing → ready_for_review
                           ↓
                  bounded repair → revalidation
```

Other outcomes include `needs_input`, `blocked_environment`, `blocked_budget`, `waiting_quota`, `failed`, `cancelled`, and `interrupted`.

Publication and merge are different operations. The public example defaults to **no automatic merging, no remote push, and no autonomous planning**. Enable each deliberately after configuring a disposable project and testing the complete deployment.

## Read the implementation

| Area | Entry point | Engineering concern |
|---|---|---|
| Job engine | [`supervisor.py`](proctor/supervisor.py) | state transitions, containers, frozen candidates, repair, recovery |
| Model/repo broker | [`broker.py`](proctor/broker.py) | credentials, reservations, quotas, publication, owner-run Git |
| Policy layer | [`orch.py`](proctor/orch.py) | capacity, review, approvals, merge controls |
| Harness adapters | [`harness.py`](proctor/harness.py) | Claude Code / Codex / scripted mock normalization |
| Registry | [`common.py`](proctor/common.py) | TOML validation, roles, paths, model approval |
| Persistence | [`db.py`](proctor/db.py) | schema, events, attempts, requests |
| CLI | [`cli.py`](proctor/cli.py) | intake, idempotency, status, cancellation |

## Scope and limitations

- Live execution targets a dedicated Linux host with Docker, systemd, and iptables compatibility. macOS/Windows live deployment is not supported by these service templates.
- Model credentials come from Anthropic/OpenAI API keys by default. Subscription credentials, subscription-quota readings and Hermes notifications need [Hermes Agent](https://github.com/NousResearch/hermes-agent) (`install.sh --hermes`). Only the Anthropic Messages and OpenAI Responses APIs are routed; this is not a general-purpose provider SDK.
- Model names, CLI versions, endpoint behavior, provider terms, and prices must be checked by the operator. No proprietary CLI binaries or provider credentials are redistributed.
- Budget figures are estimated list-price usage equivalents, not hard billing limits. In-flight requests and estimation error can overshoot; disable provider-side extra usage separately.
- Missing quota readings can permit calls unless `require_fresh` is enabled; a present reading is not continuously fresh on every path.
- Policy checks assume trusted control-plane callers and files. An approval string in a writable decision file is not cryptographic owner authentication.
- Tests in this edition cover offline behavior. The installer, live gateway, container recovery, and network isolation still need a dedicated integration test environment.

## Project status

The execution engine comes from a running private deployment. The public edition is intentionally smaller: generic example configuration, an offline demo and regression suite, CI definition, and a documented live-deployment boundary. See the [release checklist](docs/release-checklist.md) for what must be verified before claiming a public release.

[Contributing](CONTRIBUTING.md) · [Portfolio notes](docs/portfolio.md) · [MIT license](LICENSE)

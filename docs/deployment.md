# Live deployment: operator guide

The offline development path is the supported quick start. The live engine is provided as source and service templates; this guide is a manual integration checklist, **not a verified one-command installer**. Use a disposable dedicated Linux host before connecting a real project.

## Requirements

- Linux with Python 3.11+, Git, Docker Engine, systemd, `runuser`, and working IPv4/IPv6 iptables compatibility.
- A trusted administrator, a `proctor` service account, and a separate non-root account owning each source repository.
- Adequate disk headroom and container resources; example limits are configuration, not a performance benchmark.
- Hermes Agent installed through its [official instructions](https://hermes-agent.nousresearch.com/docs/), with the provider credential/quota integrations used by `broker.py` available in its Python environment.
- Operator-installed Claude Code and optionally Codex, obtained from their official distributions. These binaries are not included. The base-image build currently expects a Claude binary even for mock/Codex use; it is not a fully harness-independent bootstrap.

Read [SECURITY.md](../SECURITY.md) first. Docker-group membership grants root-equivalent authority. Do not use a shared sensitive host as an untrusted code service.

## Filesystem contract

| Location | Purpose / ownership |
|---|---|
| `/opt/proctor` | reviewed application source, root-owned, not worker-writable |
| `/etc/proctor/settings.toml` | trusted settings, typically root-owned and service-group-readable |
| `/etc/proctor/projects/<id>` | trusted registry, image recipe, brief and optional roles |
| `/var/lib/proctor` | service-owned state, job artifacts, mirrors and build context |
| `/run/proctor` | runtime lock/socket area as used by the code |
| `/run/proctor-xfer` | broker transfer area |

`PROCTOR_ETC` and `PROCTOR_STATE` override configuration/state paths for offline development. They do not make the entire live service relocatable: unit files, helper commands, runtime transfer paths, and host integrations still have deployment assumptions.

## Configure deliberately

1. Install the source at the expected application path, create the service account/group and state directories, and review the systemd templates. The supervisor requires Docker access. The broker runs as a privileged trusted service.
2. Copy the generic `config/settings.toml` to the trusted settings location. Set timezone, real harness binary paths, gateway network, conservative budget limits, quota policy, and review models. The bundled example is mock-oriented; empty model approval/price configuration is intentional, not production readiness.
3. Inspect the Hermes integration in `broker.py`: credential imports must resolve in the broker interpreter, and the Hermes executable must be available at the expected path. Review the broker unit's interpreter, home/profile, and environment for your installation. Do not copy an authentication store into this repository or a worker image.
4. Install the harness binaries under a non-worker-writable directory. `ensure_base` in `supervisor.py` stages them into a build context. Codex's matching `codex-code-mode-host` helper must be available next to the configured Codex binary using the filename convention in that function. Review external licenses before any redistribution.
5. Copy `config/projects/example` to a new registry ID and edit its matching `id`, source path, non-root owner, base branch, check commands, and dependency image. Configure all dependencies at image-build time: worker/check execution cannot download packages.
6. Leave `[orchestrator].enabled = false`, `merge = "none"`, `push = false`, and `[publish].remote_push = false` during initial verification. Start with a disposable repository and the scripted mock harness.
7. Review `bin/proctor-firewall` and gateway settings together. Its network/interface/subnet/port values must match. Ensure no subnet collision. The script changes host firewall rules: inspect it before running and retain an out-of-band recovery path.
8. Install reviewed unit files, configure an operator `proctor` CLI wrapper (or use `python3 -m proctor.cli` from the application directory), reload systemd, then enable the firewall, broker, and supervisor in dependency order.

Do not start a copied broker unit until its Hermes interpreter and credential integration are correct. Do not edit production registry or firewall settings from inside a worker.

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

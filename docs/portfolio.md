# Portfolio and interview notes

## One-sentence description

A self-hosted control plane that supervises AI coding workers with durable job state, containerized execution, independent candidate validation, usage controls, and explicit review/merge policy.

## Resume wording

Use only wording that reflects your own role and that you can explain in an interview. If AI assisted implementation, do not imply every line was manually authored.

> Designed and operated a self-hosted AI coding orchestration system using Python, SQLite, Docker, and systemd, separating worker execution from supervisor-run validation and policy-controlled repository publication.

> Implemented durable job/attempt tracking, bounded repair and recovery workflows, attempt-scoped model gateway access, and project-level budget, quota, and approval controls.

> Extracted a deployment-specific automation stack into a public development repository with a credential-free demo, isolated regression tests, architecture/threat-model documentation, and a CI workflow.

The last bullet should say “CI workflow,” not “passing hosted CI,” until the workflow actually runs in the public repository.

## A short walkthrough

1. Explain the failure mode: a worker reporting success is not validation evidence.
2. Show the architecture and identify the untrusted worker boundary.
3. Run the offline tests and control-plane demo. Explain exactly what is simulated and what executes real code.
4. Open `supervisor.py` around candidate export and validation, and `broker.py` around attempt tokens and reservation accounting.
5. Explain why a review verdict is bound to a candidate SHA and why publication is not merging.
6. Discuss an unresolved trade-off: root-equivalent Docker access, incomplete integration coverage, or file-based approval identity.

## Evidence worth adding later

Do not invent productivity or reliability numbers. Add measurements only after recording a reproducible collection method and period:

- task attempts and final dispositions;
- elapsed time by execution stage;
- repair rounds and refusal reasons;
- infrastructure recovery outcomes;
- metered usage versus provider observations;
- defects caught by independent checks before merge.

Scrub private source, project names, credentials, chat identities, and filesystem paths from any example artifacts. A generic disposable demo project is preferable to publishing real operational transcripts.

## Attribution

The orchestration/control plane in this repository is distinct from the external coding models, Claude Code, Codex, Docker, Git, and Hermes Agent that it integrates with. Credit those integrations explicitly. “Built an AI coding control plane” is more accurate than “built the coding models” or “built Hermes.”

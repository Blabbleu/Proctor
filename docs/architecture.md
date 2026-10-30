# Architecture and design trade-offs

## Control plane versus worker

The control plane owns policy, configuration, job state, and publication. The worker owns only its disposable checkout and a constrained attempt credential. Agent output is input to review, not proof of correctness.

The implementation uses Python's standard library, SQLite, Git, Docker, and systemd rather than a distributed queue or a workflow service. One supervisor serializes active execution. This keeps operational complexity low for a single-host system, at the cost of horizontal scale and high availability.

## From request to evidence

1. **Intake:** `cli.cmd_submit` validates a registered project, harness/model eligibility, acceptance text, and an optional idempotency key. Ambiguous intake becomes `needs_input` rather than silently inventing requirements.
2. **Preparation:** the broker reads the base repository as its non-root owner and creates a bundle/mirror. Trusted registry files define images, checks, protected paths, and resource limits.
3. **Baseline:** the supervisor runs the registered checks against the original base before implementation. Baseline failures are recorded separately from regressions.
4. **Implementation:** a non-root container gets a disposable checkout. Its model access uses an attempt-scoped token through the broker. The container does not receive the provider's actual OAuth credential.
5. **Freeze:** the worker's tree is exported into a fresh trusted Git checkout. This avoids using worker-edited `.git` metadata as validation evidence.
6. **Validation:** fresh containers check the frozen candidate with no network. Dependencies therefore need to be installed at image-build time. Bounded repair rounds feed concrete failures back to the worker.
7. **Publication:** the broker verifies the candidate and publishes a review branch. A patch, candidate SHA, check records, logs, and metered usage remain available for review.
8. **Landing:** the orchestration CLI enforces configured policy before asking the repository broker for a fast-forward. Remote publication and post-merge hooks require separate configuration.

A `ready_for_review` result is not a merge authorization. Baseline failures may be classified as pre-existing during validation, while merge-time policy can still refuse failing required checks. Missing or incomplete evidence must be inspected rather than inferred from a summary label.

## Persistence and recovery

[`db.py`](../proctor/db.py) stores job and attempt identities separately. Container labels and gateway credentials bind work to the current attempt. [`supervisor.py`](../proctor/supervisor.py) reattaches to surviving containers and refuses stale attempt results. A lost container can trigger an infrastructure retry within configured limits.

The orchestrator keeps project-local Markdown backlog, decisions, pending approvals, and journal alongside locked JSON state. This is easy for an operator to inspect, but it is not a multi-tenant authorization database.

A failed or cancelled run tries to export partial work. Preservation can fail; the event log reports that failure. A successful merge is not rolled back automatically if a post-merge command fails.

## Gateway and budget accounting

[`broker.py`](../proctor/broker.py) authenticates an attempt token, checks job/model compatibility, reserves estimated usage before forwarding, and records provider usage. A process-local lock protects reservations within the single broker process. This is not a distributed reservation protocol and should not be deployed as multiple independent broker replicas.

Usage-equivalent limits depend on configured prices and an input-token estimate. Subscription guards depend on provider response headers and periodic observations. Neither mechanism is a provider billing guarantee. Reviewer calls and planner calls have their own paths; a worker gateway does not automatically meter every external agent invocation.

## Autonomy and approval

[`orch.py`](../proctor/orch.py) adds task/day limits, review capacity, time-window spacing, title deduplication, role-specific research capacity, and review/approval policy. The planner can choose a task, but deterministic code controls whether it may submit or request a merge.

Review verdicts are associated with candidate SHAs. A verdict for an earlier candidate must not authorize a changed one. An explicit owner approval can override the model-review gate in the orchestration CLI; it is not permission to ignore required checks. Review models are chosen from configured lists, so “independent review” does not necessarily mean a different provider or model family.

The root broker is a trusted executor, not an independent enforcement boundary for all CLI policy. A process able to edit the control-plane database or registry can bypass assumptions. See [the threat model](../SECURITY.md).

## Deliberate trade-offs

| Choice | Benefit | Cost / boundary |
|---|---|---|
| Single host and active execution lane | simpler recovery and accounting | no high availability or fleet scheduling |
| SQLite plus local artifacts | inspectable, transactional, easy backup | host filesystem and backup consistency matter |
| Docker execution boundary | practical process/resource isolation | shared kernel; Docker daemon and host remain trusted |
| Fresh offline checks | repeatable evidence, fewer network surprises | image maintenance and preinstalled dependencies |
| Baseline-aware validation | distinguishes existing failures | not a claim that the project is healthy |
| Owner-run Git operations | avoids root-owned source trees | repository owner and credentials remain trusted |
| File-based approval workflow | auditable and easy to integrate | file permissions, not strong identity authentication |

## What would come next

A disposable Docker integration environment; authenticated approval principals; stronger merge evidence completeness checks; reduced broker privilege; explicit credential/provider interfaces; pinned image/harness supply-chain inputs; and measured operational metrics. These are future work, not capabilities demonstrated by the offline demo.

# Security model

## Intended use

A single-owner development automation system on a dedicated Linux host. Do not expose the broker to the public internet, put unrelated secrets in a worker workspace, or treat this project as a hardened multi-tenant execution service.

## Trust boundaries

**Trusted:** host administrator, Docker daemon, broker, supervisor, registry files and check commands, state database, repository owner, and planner/operator with control-plane write access.

**Untrusted or fallible:** worker-generated source changes, worker reports and Git metadata, model output, and repository content supplied to an agent. Registered checks themselves are trusted configuration, although they execute repository code.

The supervisor belongs to the Docker group. Docker access is effectively host-root access. Non-root service identities improve file hygiene but do not isolate the broker from a compromised supervisor.

## Implemented defenses

- Non-root worker containers, dropped capabilities, read-only root filesystem, and configured CPU/memory/PID limits.
- Internal runner network and host firewall rules intended to allow model-gateway access only.
- Offline check containers.
- Attempt-scoped gateway tokens, stale-attempt checks, approved model lists, estimated budget reservations, and quota observations.
- Trusted candidate export, protected paths and role restrictions, and publication tied to a candidate commit.
- Policy-controlled CLI merge requests and explicit operator approvals.

These are implementation mechanisms, not an independent security certification. Verify actual firewall behavior on the deployment host. Container escapes, kernel vulnerabilities, Docker API access, writable host mounts, and trusted-control-plane compromise remain in scope for an operator's risk assessment.

## Important limitations

- The broker executes privileged filesystem and repository operations. Its local database/request channel is trusted. Not every CLI-level merge policy is re-validated inside the broker.
- Owner approval is parsed from a trusted decision file; it is not signed authentication. Do not give untrusted agents unrestricted access to that file, the registry, or the job database.
- Missing quota data can allow requests unless configured otherwise. Stale observations, in-flight usage, pricing changes, and token estimates prevent hard spending guarantees.
- Model review is fallible and can be explicitly overridden by owner approval. Check logs, exact candidate identities, and evidence completeness still matter.
- Artifact logs and patches can contain sensitive source or model output. Treat them as private until reviewed. Never commit runtime state, provider auth stores, access tokens, or real project registries.
- Worker network controls do not mean the trusted host has no network access. Image builds, providers, optional publication, and notification use the network.
- Base image and harness inputs are not fully supply-chain pinned by this edition.

## Reporting an issue

For a non-sensitive bug, open a repository issue with a minimal reproduction that uses synthetic data. For a potential vulnerability or leaked credential, use the repository's private security reporting channel if enabled. If private reporting is unavailable, contact the maintainer through their public profile without posting exploit details or secret values. Never attach real credentials, production databases, or unredacted worker transcripts.

Before public release, enable GitHub private vulnerability reporting and select a monitored maintainer contact. No response-time guarantee is currently offered.

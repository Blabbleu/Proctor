# Public release checklist

A local candidate is not a published release. Keep these gates explicit.

## Local engineering gates

- [ ] Offline suite passes on the final candidate.
- [ ] Demo runs without model credentials, Docker, or live services.
- [ ] Tests also pass as an ordinary non-root Linux user from an unrelated checkout path.
- [ ] Source hygiene guard passes; full staged diff reviewed.
- [ ] Independent credential scan covers the complete public tree and Git history.
- [ ] Independent review findings resolved or explicitly disclosed.
- [ ] Generic example config validates and defaults to no autonomous planning, merge, or push.
- [ ] Documentation distinguishes offline behavior from live integration coverage.
- [ ] MIT license and external integration attribution are present.

## Repository-owner actions

- [ ] Choose the GitHub owner/repository name and approve upload.
- [ ] Confirm source provenance and right to publish all included material.
- [ ] Push only the sanitized public history; never push the private deployment's history by accident.
- [ ] Enable private vulnerability reporting and a monitored maintainer contact.
- [ ] Run hosted CI and resolve failures on all declared Python versions.
- [ ] Configure branch protection and required checks after their first successful run.
- [ ] Review GitHub secret-scanning alerts.
- [ ] Record a short demo and pin the repository on your profile.

## Before claiming live reproducibility

- [ ] Provision a disposable dedicated Linux host with no production projects.
- [ ] Install supported harnesses through their official distribution, respecting licenses and provider terms.
- [ ] Configure and test Hermes credential/quota adapters without exposing tokens.
- [ ] Validate internal-network, host-access, IPv4 and IPv6 firewall behavior.
- [ ] Run complete mock-harness jobs through preparation, worker, export, checks, and branch publication.
- [ ] Exercise validation failure, bounded repair, role violation, cancellation, restart/reattachment, lost container, and stale-attempt refusal.
- [ ] Exercise required-check failures and missing evidence before any merge.
- [ ] Test optional provider calls and publication only with explicit operator approval.

Unchecked items are open work, not implied passes. Consult `verification.md` for the actual local verification record, when supplied.

# Local verification record

This record describes checks actually run against the public development edition. It is not a live-engine certification or a hosted CI result.

## Executed checks

- Python 3.13.5: `python3 -m unittest discover -s tests -v` — **22 tests passed**.
- Same suite from `/repo` as UID/GID 10001, with the source mounted read-only, no network, dropped capabilities and a read-only container root — **22 tests passed**. An existing development base image supplied Python/Git; this was not a from-scratch installation test.
- `python3 examples/control_plane_demo.py` — one real temporary SQLite job queued, duplicate intake reused its ID, and the actual capacity function refused another active job. No coding worker, provider, gateway, or supervisor was started by the demo.
- `make check` — Python compilation, shell syntax and source-hygiene checks passed.
- Source-hygiene guard with an external operator-only identifier policy — index and worktree passed. The policy is intentionally outside the repository.
- Gitleaks 8.30.1 directory scan with default rules and redacted output — no leaks found. This is pattern-based evidence, not proof of absence of secrets.
- `git diff --cached --check` — no whitespace errors.

The quota-refresh failure fixture deliberately emits warning messages from mocked failures. These do not indicate a live provider call.

## Release-review coverage

The first independent review found private deployment traces and index-scanning false negatives. The corrected scanner reads staged blobs as well as working files, does not skip tracked cache paths, rejects tracked harness directories, and supports an external private policy. Regression tests cover these cases, staged removal, symlinks, invalid policy input, and output redaction.

The corrected candidate still requires final independent review before being described as approved. Any review result must be associated with its exact Git tree or commit; adding this document changes that identity.

## Not verified

- Hosted GitHub Actions or local Python 3.11/3.12 execution.
- A new host installation or standalone package installation.
- Live provider authentication, pricing accuracy, subscription quotas or notifications.
- Full worker/supervisor execution, candidate export or branch publication.
- Container recovery, cancellation, network/firewall isolation or merge enforcement end to end.

Keep these limitations visible when presenting the project. The public quick start demonstrates the offline control plane, not the entire live deployment.

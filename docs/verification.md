# Local verification record

This record describes checks actually run against the public development edition. It is not a live-engine certification.

Candidate: `8a68c98` (post-rename `main`) plus the demo fix and the install/onboarding changes committed with this record. Checks were run on 2026-10-30.

## Executed checks

- Host, Python 3.13.5: `python3 -m unittest discover -s tests -v` — **38 tests passed** (22 existing, 16 covering settings defaults, credential sources, notifications and `proctor project add`).
- Before the install/onboarding changes: Python 3.11.18, 3.12.16 and 3.13.18 (`python:<version>-slim` with Debian Git 2.47.3 added) as UID/GID 10001 with no passwd entry, the source mounted read-only, no network, all capabilities dropped and a read-only container root — **22 tests passed and the demo ran on each**. These images are not a from-scratch host installation test.
- `python3 examples/control_plane_demo.py` — one real temporary SQLite job queued, duplicate intake reused its ID, and the actual capacity function refused another active job. No coding worker, provider, gateway, or supervisor was started by the demo.
- `make check` — Python compilation, shell syntax and source-hygiene checks passed.
- Gitleaks 8.30.1 (official container, no network) with default rules and redacted output — full Git history (2 commits) and directory scans found no leaks. This is pattern-based evidence, not proof of absence of secrets.
- `git diff --check` — no whitespace errors.
- ShellCheck 0.10.0 at warning level on `install.sh` and `bin/proctor-firewall` — no findings.
- `sudo ./install.sh --dry-run` on a host already running another deployment — preflight passed, the default gateway subnet was detected as routed and the next free one chosen, and every planned action was printed without changes. The generated `settings.toml` substitutions were parsed back through `load_settings()` and `gateway-env`.
- No `swe-pilot`/`swe_pilot` references remain in the tracked tree after the rename.
- Hosted GitHub Actions "Offline checks" (Python 3.11–3.13) succeeded on both pushed commits, `46bbfb6` and `8a68c98`.

The quota-refresh failure fixture deliberately emits warning messages from mocked failures. These do not indicate a live provider call.

## Fixed during this verification

The demo crashed with `KeyError` from `pwd.getpwuid()` when run under a UID with no passwd entry, as is common in containers and CI sandboxes. It now falls back to the `developer` placeholder owner, the same value it already uses under root. The earlier non-root run passed only because its base image had a passwd entry for that UID.

## Release-review coverage

The first independent review found private deployment traces and index-scanning false negatives. The corrected scanner reads staged blobs as well as working files, does not skip tracked cache paths, rejects tracked harness directories, and supports an external private policy. Regression tests cover these cases, staged removal, symlinks, invalid policy input, and output redaction.

The source-hygiene guard was not re-run with the external operator-only identifier policy for this candidate; that policy is kept outside this repository and was not used here.

The candidate still requires final independent review before being described as approved. Any review result must be associated with its exact Git tree or commit; adding this document changes that identity.

## Not verified

- A real `install.sh` run (install, upgrade, `--hermes`, `--uninstall`, `--purge`) on a fresh host.
- Live API-key (`[credentials] = "env"`) requests to the Anthropic Messages or OpenAI Responses APIs, and webhook delivery to a real endpoint.
- The non-root Python 3.11–3.13 container matrix on the install/onboarding changes (hosted CI covers 3.11–3.13 as its default user).
- Standalone package installation (`pip install` / the `proctor` console script).
- Live provider authentication, pricing accuracy, subscription quotas or notifications.
- Full worker/supervisor execution, candidate export or branch publication.
- Container recovery, cancellation, network/firewall isolation or merge enforcement end to end.

Keep these limitations visible when presenting the project. The public quick start demonstrates the offline control plane, not the entire live deployment.

# Contributing

Start with the offline path; it must not require credentials, root, Docker, a live broker, or a registered private project.

```bash
python3 -m unittest discover -s tests -v
python3 examples/control_plane_demo.py
python3 scripts/check_public_release.py
python3 -m compileall -q swe_pilot examples scripts tests
```

Use Python 3.11+ on Linux. Keep core runtime dependencies in the standard library unless a dependency has a clear operational benefit. Do not make import-time network calls or open deployment paths in tests.

## Change expectations

- Reproduce a bug with a failing behavior test before fixing it.
- Keep worker claims separate from supervisor evidence.
- Test stale identities, repeated actions, refusal paths, and interrupted state transitions—not just success.
- Use temporary config/state paths and synthetic fixtures. Never run production gateway/reboot scripts as unit tests.
- Keep live execution opt-in. State precisely which environment and candidate were exercised.
- Treat registry commands, provider credentials, and the Docker daemon as privileged inputs.
- Do not weaken a check, delete a failing test, enable remote push, or bypass approval merely to obtain a green run.
- Submit focused pull requests explaining the observed behavior, expected behavior, tests, and remaining limitations.

The public-release hygiene guard is heuristic, not a secret scanner. Review the complete staged diff and run an independent credential scan before publication. Proprietary harness binaries and personal deployment files do not belong in the repository.

The guard checks index blobs and the worktree, including untracked files except those ignored by Git. For deployment-specific identifiers, keep an operator-only JSON policy outside the repository and pass `--policy PATH`. Its format is `{"forbidden_strings": ["synthetic-example"]}`: nonempty strings are matched case-insensitively as plain substrings in filenames and content. Diagnostics do not print matching values. Never commit the policy. Symlinks, gitlinks, binary files and unreadable or ambiguous inputs require manual review; a clean heuristic scan is not proof of safety.

Contributions are provided under the project's MIT license. External components retain their own licenses and terms; do not copy code or redistribute binaries without permission.

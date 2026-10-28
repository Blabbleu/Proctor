#!/usr/bin/env python3
"""Heuristic release hygiene check; not a replacement for a secret scanner."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT_PATH = re.compile('/' + r'root(?=/|[\s"\']|$)')
SENSITIVE_NAMES = re.compile(r'(^|/)(\.env(?:\..*)?|id_(?:rsa|dsa|ecdsa|ed25519)|credentials?(?:\..*)?|auth\.json|state\.json|[^/]+\.(?:pem|key|p12|pfx|crt|sqlite3?|db))$', re.I)
CREDENTIAL = re.compile(r'(?:sk-' + r'[A-Za-z0-9_-]{20,}|gh[pousr]_' + r'[A-Za-z0-9]{20,}|github_pat_' + r'[A-Za-z0-9_]{20,}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:api[_-]?key|access[_-]?token|auth[_-]?token)\s*[=:]\s*[\"\'][A-Za-z0-9_./+-]{20,})', re.I)


def inspect_content(rel, raw, policy=()):
    name = rel.as_posix()
    problems = []
    if SENSITIVE_NAMES.search(name) or any(x in name.lower() for x in policy):
        problems.append('sensitive or private filename')
    if any(part.lower() in {'harness', 'harnesses', 'harness-bin'} for part in rel.parts[:-1]):
        problems.append('harness binary directory')
    if b'\0' in raw:
        problems.append('binary file requires manual review')
    text = raw.decode('utf-8', 'replace')
    if any(x in text.lower() for x in policy) or ROOT_PATH.search(text):
        problems.append('private identifier or absolute home path')
    if CREDENTIAL.search(text):
        problems.append('credential-like content')
    return problems


def inspect_file(root, rel, policy=()):
    path = root / rel
    if any((root / Path(*rel.parts[:i])).is_symlink() for i in range(1, len(rel.parts) + 1)):
        return ['symlink requires manual review']
    try:
        return inspect_content(rel, path.read_bytes(), policy)
    except OSError:
        return ['unreadable file']


def git(root, *args):
    return subprocess.run(['git', '-C', str(root), *args], capture_output=True, check=True).stdout


def records(data):
    if data and not data.endswith(b'\0'):
        raise ValueError('unterminated enumeration')
    return data[:-1].split(b'\0') if data else []


def relative(raw):
    rel = Path(os.fsdecode(raw))
    if not raw or rel.is_absolute() or any(part in ('.', '..') for part in rel.parts) or rel.as_posix() != os.fsdecode(raw):
        raise ValueError('invalid path')
    return rel


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--policy', type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        policy = ()
        if args.policy:
            obj = json.loads(args.policy.read_text(encoding='utf-8'))
            if not isinstance(obj, dict) or set(obj) != {'forbidden_strings'} or not isinstance(obj['forbidden_strings'], list) or any(not isinstance(x, str) or not x for x in obj['forbidden_strings']):
                raise ValueError('invalid policy')
            policy = tuple(x.lower() for x in obj['forbidden_strings'])
        entries = []
        seen = set()
        for record in records(git(root, 'ls-files', '--stage', '-z')):
            meta, raw = record.split(b'\t', 1)
            mode, oid, stage = meta.split(b' ')
            rel = relative(raw)
            if rel in seen or stage != b'0' or mode not in (b'100644', b'100755', b'120000', b'160000') or not re.fullmatch(b'[0-9a-f]{40}|[0-9a-f]{64}', oid):
                raise ValueError('invalid index entry')
            seen.add(rel)
            entries.append((rel, mode, oid))
        others = [relative(raw) for raw in records(git(root, 'ls-files', '--others', '--exclude-standard', '-z'))]
        if len(set(others)) != len(others) or seen.intersection(others):
            raise ValueError('invalid untracked enumeration')
        failed = False
        def report(rel, errors, source):
            nonlocal failed
            if errors:
                failed = True
                name = str(rel)
                label = '[redacted filename]' if CREDENTIAL.search(name) or any(x in name.lower() for x in policy) else name
                print(f'{label} ({source}): {", ".join(errors)}')
        for rel, mode, oid in entries:
            if mode == b'160000':
                report(rel, ['gitlink requires manual review'], 'index')
            else:
                raw = git(root, 'cat-file', 'blob', oid.decode('ascii'))
                errors = inspect_content(rel, raw, policy)
                if mode == b'120000':
                    errors.append('symlink requires manual review')
                report(rel, errors, 'index')
            report(rel, inspect_file(root, rel, policy), 'worktree')
        for rel in others:
            report(rel, inspect_file(root, rel, policy), 'worktree')
    except (OSError, ValueError, UnicodeError, subprocess.SubprocessError):
        print('Unable to read policy or enumerate and inspect Git files', file=sys.stderr)
        return 2
    print('Release hygiene: ' + ('FAIL' if failed else 'PASS') + ' (heuristic only)')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())

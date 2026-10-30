"""Offline control-plane and release guard regression tests."""
import atexit
import argparse
import ast
import json
import os
from pathlib import Path
import subprocess
import sqlite3
import sys
import tempfile
import tomllib
import unittest
from datetime import datetime, timezone
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
_sandbox = tempfile.TemporaryDirectory()
atexit.register(_sandbox.cleanup)
os.environ['PROCTOR_ETC'] = str(Path(_sandbox.name) / 'etc')
os.environ['PROCTOR_STATE'] = str(Path(_sandbox.name) / 'state')
sys.path.insert(0, str(ROOT))
from proctor import broker, cli, common, db, harness, orch


class OfflineBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.etc = Path(self.tmp.name) / 'etc'
        self.project_dir = self.etc / 'projects' / 'example'
        self.project_dir.mkdir(parents=True)
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(common, 'ETC', self.etc).start()
        (self.etc / 'settings.toml').write_text('[prices]\n')
        self.project_file = self.project_dir / 'project.toml'
        self.project_file.write_text((ROOT / 'config/projects/example/project.toml').read_text())
        (self.project_dir / 'orchestrator.md').write_text('Example brief')
        self.conn = sqlite3.connect(':memory:', isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.addCleanup(self.conn.close)
        db.init(self.conn)

    def test_sample_load_and_validation(self):
        p = common.load_project('example')
        self.assertFalse(p['orchestrator']['enabled'])
        self.assertEqual(p['orchestrator']['merge'], 'none')
        self.assertFalse(p['orchestrator']['push'])
        with self.assertRaises(common.ProjectError):
            common.load_project('../example')
        self.project_file.write_text(self.project_file.read_text().replace('owner = "developer"', 'owner = "root"'))
        with self.assertRaises(common.ProjectError):
            common.load_project('example')

    def test_watchdog_example_is_toml(self):
        doc = ast.get_docstring(ast.parse((ROOT / 'bin/proctor-devops-check').read_text()))
        assert doc is not None
        example = '[[service]]' + doc.split('    [[service]]', 1)[1]
        self.assertEqual(set(tomllib.loads(example)), {'service', 'http', 'fresh', 'log_errors'})

    def test_role_paths_and_prompt(self):
        self.project_file.write_text(self.project_file.read_text() + '\n[roles_common]\nshared_paths = ["tests/*"]\n[roles.dev]\nbrief = "orchestrator.md"\npaths = ["src/*"]\n')
        p = common.load_project('example')
        self.assertEqual(common.role_violations(p, 'dev', ['src/app.py', 'tests/test.py', 'secret.txt']), ['secret.txt'])
        j = {'id': 'j0101-abcdef', 'title': 'Sample', 'goal': 'Make a change', 'acceptance': json.dumps(['It works']), 'base_branch': 'main', 'base_commit': 'abc', 'meta': '{"role":"dev"}'}
        prompt = harness.task_prompt(j, p, [])
        for expected in ('It works', 'Example brief', 'src/*', 'tests/*', 'supervisor'):
            self.assertIn(expected, prompt)
        self.project_file.write_text(self.project_file.read_text().replace('src/*', '../*'))
        with self.assertRaises(common.ProjectError):
            common.load_project('example')

    def test_schema_idempotent(self):
        db.init(self.conn)
        self.assertIn('jobs', {r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")})

    def test_quota_subprocess_uses_configured_home(self):
        (self.etc / 'settings.toml').write_text('[prices]\n[credentials]\nanthropic = "hermes"\n')
        reply = mock.Mock(stdout='{"windows": [{"label": "session", "used_percent": 1}]}')
        with mock.patch.dict(os.environ, {'HOME': '/srv/example-home'}), mock.patch.object(broker.subprocess, 'run', return_value=reply) as run:
            broker.handle_quota(self.conn, None, {'provider': 'anthropic'})
        self.assertEqual(run.call_args.kwargs['env']['HOME'], '/srv/example-home')

    def test_notification_subprocess_uses_configured_home(self):
        self.conn.execute('INSERT INTO outbox(created_at, body) VALUES (?, ?)', (1, 'example notification'))
        with mock.patch.dict(os.environ, {'HOME': '/srv/example-home'}), mock.patch.object(broker, 'load_settings', return_value={'notify': {'target': 'example'}}), mock.patch.object(broker.subprocess, 'run', return_value=mock.Mock(returncode=0)) as run:
            broker.send_outbox(self.conn)
        self.assertEqual(run.call_args.kwargs['env']['HOME'], '/srv/example-home')
        self.assertEqual(self.conn.execute('SELECT status FROM outbox').fetchone()[0], 'sent')

    def test_capacity_daily_spacing_night_and_research(self):
        p = common.load_project('example')
        o = p['orchestrator']
        o.update(max_tasks_per_day=1, max_open_reviews=3, day_gap_hours=3, research_tasks_per_night=1)
        t = datetime(2025, 1, 1, 12, tzinfo=timezone.utc).timestamp()
        st = {'enabled_at': t - 10000, 'handled': {}, 'submitted': [{'ts': t - 60}]}
        view = mock.Mock()
        cap = orch.capacity(self.conn, {'timezone': 'UTC'}, 'example', p, st, view, 'mock', t)
        self.assertFalse(cap['ok'])
        self.assertTrue(any('tasks submitted today' in r for r in cap['reasons']))
        self.assertTrue(any('spacing:' in r for r in cap['reasons']))
        night = datetime(2025, 1, 1, 23, tzinfo=timezone.utc).timestamp()
        self.assertTrue(orch.in_night(night, '23-05', {'timezone': 'UTC'}))
        self.assertFalse(orch.research_capacity(st, p, {'timezone': 'UTC'}, t)[0])
        self.assertTrue(orch.research_capacity(st, p, {'timezone': 'UTC'}, night)[0])
        st['submitted'].append({'ts': night - 10, 'lane': 'research'})
        self.assertFalse(orch.research_capacity(st, p, {'timezone': 'UTC'}, night)[0])

    def test_review_matches_candidate_and_approval(self):
        job = {'candidate_commit': 'abc', 'meta': json.dumps({'review': {'candidate_commit': 'abc', 'verdict': 'approve'}})}
        self.assertEqual(orch.stored_review(job)['verdict'], 'approve')
        job['candidate_commit'] = 'other'
        self.assertIsNone(orch.stored_review(job))
        self.assertTrue(orch.approval_granted('approve A3', 'A3'))
        self.assertFalse(orch.approval_granted('do not approve A3', 'A3'))
        self.assertFalse(orch.approval_granted('approve A30', 'A3'))

    def test_merge_none_refuses_before_broker(self):
        args = argparse.Namespace(project='example', goal='Make a useful example change', accept=['The example behaves correctly'], title='Example', harness='mock', model=None, mock='good', key='merge-test', quiet=True, retry_of=None, role=None, json=True)
        job = orch.run_cli(cli.cmd_submit, args, self.conn)
        p = common.load_project('example')
        with mock.patch.object(orch, 'need_orch', return_value=p), mock.patch.object(orch, 'broker_request') as broker:
            with self.assertRaisesRegex(orch.OrchError, 'merge refused'):
                orch.cmd_merge(argparse.Namespace(job=job['id'], approved=None), self.conn)
            broker.assert_not_called()

    def test_demo(self):
        run = subprocess.run([sys.executable, str(ROOT / 'examples/control_plane_demo.py'), '--json'], capture_output=True, text=True, check=True)
        result = json.loads(run.stdout)
        self.assertEqual(result['initial']['state'], 'queued')
        self.assertTrue(result['duplicate']['duplicate'])
        self.assertEqual(result['initial']['id'], result['duplicate']['id'])
        self.assertFalse(result['capacity']['ok'])
        self.assertEqual(result['job_count'], 1)


class HygieneTests(unittest.TestCase):
    def test_guard_detects_synthetic_names_paths_and_secrets(self):
        from scripts.check_public_release import inspect_file
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            for name, data in [('ok.txt', 'hello'), ('id' + '_rsa', 'irrelevant'), ('key.txt', 'sk-' + 'x' * 32), ('path.txt', '/' + 'root/secret'), ('home.txt', '/' + 'root')]:
                (d / name).write_text(data)
            self.assertEqual(inspect_file(d, Path('ok.txt')), [])
            for name in ('id' + '_rsa', 'key.txt', 'path.txt', 'home.txt'):
                self.assertTrue(inspect_file(d, Path(name)), name)

    def test_guard_scans_git_untracked_and_reports_no_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            subprocess.run(['git', 'init', '-q', tmp], check=True)
            secret = 'sk-' + 'z' * 32
            Path(tmp, 'leak.txt').write_text(secret)
            r = subprocess.run([sys.executable, str(ROOT / 'scripts/check_public_release.py'), '--root', tmp], capture_output=True, text=True)
            self.assertNotEqual(r.returncode, 0)
            self.assertIn('leak.txt', r.stdout)
            self.assertNotIn(secret, r.stdout + r.stderr)

    def test_index_worktree_policy_and_deletions(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            d = base / 'repo'
            d.mkdir()
            subprocess.run(['git', 'init', '-q', str(d)], check=True)
            policy = base / 'policy.json'
            secret = 'synthetic-private-identity'
            policy.write_text(json.dumps({'forbidden_strings': [secret]}))
            def git(*args):
                subprocess.run(['git', '-C', str(d), *args], check=True, capture_output=True)
            def scan():
                return subprocess.run([sys.executable, str(ROOT / 'scripts/check_public_release.py'), '--root', str(d), '--policy', str(policy)], capture_output=True, text=True)
            def rejected():
                result = scan()
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertNotIn(secret, result.stdout + result.stderr)
            f = d / 'file.txt'
            f.write_text('clean')
            git('add', 'file.txt')
            self.assertEqual(scan().returncode, 0)
            f.write_text(secret)
            git('add', 'file.txt')
            f.write_text('clean')
            rejected()  # index differs from worktree
            git('add', 'file.txt')
            f.write_text(secret)
            rejected()  # dirty worktree
            f.write_text('clean')
            cache = d / '__pycache__'
            cache.mkdir()
            (cache / 'leak.txt').write_text(secret)
            git('add', '-f', '__pycache__/leak.txt')
            rejected()
            git('rm', '--cached', '-q', '__pycache__/leak.txt')
            (cache / 'leak.txt').unlink()
            (d / (secret + '.txt')).write_text('clean')
            rejected()  # policy in filename is redacted
            (d / (secret + '.txt')).unlink()
            git('rm', '-f', '-q', 'file.txt')
            self.assertEqual(scan().returncode, 0)  # staged deletion

    def test_index_credentials_and_symlink_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            subprocess.run(['git', 'init', '-q', str(d)], check=True)
            secret = 'sk-' + 'Q' * 32
            def git(*args):
                subprocess.run(['git', '-C', str(d), *args], check=True, capture_output=True)
            def scan():
                return subprocess.run([sys.executable, str(ROOT / 'scripts/check_public_release.py'), '--root', str(d)], capture_output=True, text=True)
            f = d / 'file.txt'
            f.write_text(secret)
            git('add', 'file.txt')
            f.write_text('clean')
            r = scan()
            self.assertEqual(r.returncode, 1)
            self.assertIn('credential-like content', r.stdout)
            self.assertNotIn(secret, r.stdout + r.stderr)
            git('add', 'file.txt')
            (d / '__pycache__').mkdir()
            (d / '__pycache__' / 'leak.txt').write_text(secret)
            git('add', '-f', '__pycache__/leak.txt')
            r = scan()
            self.assertEqual(r.returncode, 1)
            self.assertIn('credential-like content', r.stdout)
            git('rm', '-f', '-q', '__pycache__/leak.txt')
            (d / 'link').symlink_to('file.txt')
            git('add', 'link')
            self.assertEqual(scan().returncode, 1)

    def test_invalid_policy_fails_without_echoing_contents(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            subprocess.run(['git', 'init', '-q', str(d)], check=True)
            policy = d / 'bad-policy'
            secret = 'synthetic-policy-value'
            policy.write_text(secret)
            r = subprocess.run([sys.executable, str(ROOT / 'scripts/check_public_release.py'), '--root', str(d), '--policy', str(policy)], capture_output=True, text=True)
            self.assertEqual(r.returncode, 2)
            self.assertNotIn(secret, r.stdout + r.stderr)

    def test_forced_tracked_harness_directory_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            subprocess.run(['git', 'init', '-q', str(d)], check=True)
            (d / '.gitignore').write_text((ROOT / '.gitignore').read_text())
            (d / 'harness').mkdir()
            (d / 'harness' / 'binary.txt').write_text('clean')
            ignored = subprocess.run(['git', '-C', str(d), 'check-ignore', '-q', 'harness/binary.txt'])
            self.assertEqual(ignored.returncode, 0)
            subprocess.run(['git', '-C', str(d), 'add', '-f', 'harness/binary.txt'], check=True)
            r = subprocess.run([sys.executable, str(ROOT / 'scripts/check_public_release.py'), '--root', str(d)], capture_output=True, text=True)
            self.assertEqual(r.returncode, 1, r.stdout + r.stderr)


if __name__ == '__main__':
    unittest.main()

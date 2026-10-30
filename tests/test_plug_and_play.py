"""Offline tests for the out-of-the-box path: settings defaults, credential sources, notifications, onboarding."""
import atexit
import argparse
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
_sandbox = tempfile.TemporaryDirectory()
atexit.register(_sandbox.cleanup)
os.environ['PROCTOR_ETC'] = str(Path(_sandbox.name) / 'etc')
os.environ['PROCTOR_STATE'] = str(Path(_sandbox.name) / 'state')
sys.path.insert(0, str(ROOT))
from proctor import broker, cli, common, db  # noqa: E402


class SandboxCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.etc = Path(self.tmp.name) / 'etc'
        (self.etc / 'projects').mkdir(parents=True)
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(common, 'ETC', self.etc).start()
        mock.patch.object(cli, 'ETC', self.etc).start()
        self.write_settings('')
        self.conn = sqlite3.connect(':memory:', isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.addCleanup(self.conn.close)
        db.init(self.conn)

    def write_settings(self, text):
        (self.etc / 'settings.toml').write_text(text)


class SettingsTests(SandboxCase):
    def test_empty_file_gets_every_default(self):
        s = common.load_settings()
        self.assertEqual(s['gateway']['port'], 18780)
        self.assertEqual(s['credentials'], {'anthropic': 'env', 'openai': 'env'})
        self.assertEqual(s['harness']['claude_binary'], '')
        self.assertEqual(s['notify']['method'], 'none')

    def test_shipped_settings_match_defaults(self):
        self.write_settings((ROOT / 'config/settings.toml').read_text())
        s = common.load_settings()
        for table in ('gateway', 'credentials', 'harness', 'notify'):
            self.assertEqual(s[table], common.SETTINGS_DEFAULTS[table], table)

    def test_partial_table_keeps_values_and_fills_the_rest(self):
        self.write_settings('[gateway]\nport = 19000\n')
        gw = common.load_settings()['gateway']
        self.assertEqual((gw['port'], gw['bind']), (19000, '172.31.250.1'))

    def test_invalid_values_are_refused(self):
        for text in ('[credentials]\nanthropic = "vault"\n', '[notify]\nmethod = "pager"\n',
                     '[notify]\nmethod = "webhook"\nurl = "file:///etc/shadow"\n', 'gateway = 1\n'):
            self.write_settings(text)
            with self.assertRaises(ValueError, msg=text):
                common.load_settings()

    def test_legacy_notify_target_means_hermes(self):
        self.write_settings('[notify]\ntarget = "ops"\n')
        self.assertEqual(common.load_settings()['notify']['method'], 'hermes')

    def test_gateway_env_is_quoted_and_validated(self):
        self.write_settings('[gateway]\nsubnet = "10.9.8.0/24"\nbind = "10.9.8.1"\n')
        env = dict(line.split('=', 1) for line in common.gateway_env(common.load_settings()).splitlines())
        self.assertEqual((env['SUBNET'], env['GW'], env['PORT']), ('10.9.8.0/24', '10.9.8.1', '18780'))
        self.write_settings('[gateway]\nsubnet = "10.9.8.0/24"\nbind = "10.9.9.1"\n')
        with self.assertRaises(ValueError):
            common.gateway_env(common.load_settings())
        self.write_settings('[gateway]\ninterface = "x; rm -rf /"\n')
        with self.assertRaises(ValueError):
            common.gateway_env(common.load_settings())


class CredentialTests(SandboxCase):
    def test_env_source_uses_api_keys_and_public_endpoints(self):
        s = common.load_settings()
        with mock.patch.dict(os.environ, {'ANTHROPIC_API_KEY': 'a-key', 'OPENAI_API_KEY': 'o-key'}):
            up, cx = broker.Upstream(s), broker.CodexUpstream(s)
            self.assertEqual(up.token(), 'a-key')
            self.assertEqual(cx.token(), 'o-key')
        self.assertFalse(up.oauth)
        self.assertEqual(up.auth_headers('a-key'), {'x-api-key': 'a-key'})
        self.assertEqual((cx.host, cx.path), ('api.openai.com', '/v1/responses'))
        self.assertEqual(cx.headers('o-key'), {'authorization': 'Bearer o-key'})

    def test_missing_key_resolves_to_none(self):
        with mock.patch.dict(os.environ, {'ANTHROPIC_API_KEY': '  '}, clear=False):
            os.environ.pop('OPENAI_API_KEY', None)
            s = common.load_settings()
            self.assertIsNone(broker.Upstream(s).token())
            self.assertIsNone(broker.CodexUpstream(s).token())

    def test_hermes_source_keeps_subscription_shape(self):
        self.write_settings('[credentials]\nanthropic = "hermes"\nopenai = "hermes"\n[hermes]\nagent_dir = "/nonexistent/hermes"\n')
        s = common.load_settings()
        up, cx = broker.Upstream(s), broker.CodexUpstream(s)
        self.assertTrue(up.oauth)
        self.assertEqual(up.auth_headers('t'), {'authorization': 'Bearer t'})
        self.assertEqual((cx.host, cx.path), ('chatgpt.com', '/backend-api/codex/responses'))
        self.assertIn('/nonexistent/hermes', sys.path)
        sys.path.remove('/nonexistent/hermes')

    def test_quota_needs_hermes_and_refresh_stays_quiet(self):
        with mock.patch.object(broker.subprocess, 'run') as run:
            with self.assertRaises(broker.QuotaUnavailable):
                broker.handle_quota(self.conn, None, {'provider': 'anthropic'})
            with self.assertNoLogs(broker.log, level='WARNING'):
                self.assertFalse(broker.refresh_quota_if_stale(self.conn, 'anthropic'))
        run.assert_not_called()


class NotifyTests(SandboxCase):
    def queue(self):
        self.conn.execute('INSERT INTO outbox(created_at, body) VALUES (?, ?)', (1, 'job done'))

    def status(self):
        return self.conn.execute('SELECT status, attempts FROM outbox').fetchone()

    def test_default_skips(self):
        self.queue()
        broker.send_outbox(self.conn)
        self.assertEqual(self.status()['status'], 'skipped')

    def test_webhook_posts_json(self):
        self.write_settings('[notify]\nmethod = "webhook"\nurl = "https://hooks.example.test/x"\n')
        self.queue()
        resp = mock.MagicMock(status=204)
        resp.__enter__.return_value = resp
        with mock.patch.object(broker.urllib.request, 'urlopen', return_value=resp) as urlopen:
            broker.send_outbox(self.conn)
        req = urlopen.call_args.args[0]
        self.assertEqual(req.full_url, 'https://hooks.example.test/x')
        self.assertIn(b'"text": "job done"', req.data)
        self.assertEqual(self.status()['status'], 'sent')

    def test_webhook_failure_is_retried(self):
        self.write_settings('[notify]\nmethod = "webhook"\nurl = "https://hooks.example.test/x"\n')
        self.queue()
        with mock.patch.object(broker.urllib.request, 'urlopen', side_effect=OSError('unreachable')):
            broker.send_outbox(self.conn)
        self.assertEqual(tuple(self.status()), ('pending', 1))


class CliDefaultsTests(SandboxCase):
    def test_usage_runs_on_shipped_settings(self):
        self.write_settings((ROOT / 'config/settings.toml').read_text())
        with mock.patch('builtins.print') as p:
            cli.cmd_usage(argparse.Namespace(json=False), self.conn)
        self.assertIn('quota guard: off', ' '.join(str(c.args[0]) for c in p.call_args_list))


class OnboardingTests(SandboxCase):
    def add(self, **kw):
        args = dict(id='demo', path=str(self.repo), owner='developer', branch=None, name=None, check=None,
                    harness='mock', model=None, force=False)
        args.update(kw)
        return cli.cmd_project_add(argparse.Namespace(**args), self.conn)

    def setUp(self):
        super().setUp()
        self.repo = Path(self.tmp.name) / 'repo'
        (self.repo / '.git').mkdir(parents=True)
        (self.repo / '.git' / 'HEAD').write_text('ref: refs/heads/trunk\n')

    def test_registers_a_valid_project(self):
        with mock.patch('builtins.print'):
            self.add(check=['unit=python3 -m unittest -q', 'lint = ruff check .'])
        p = common.load_project('demo')
        self.assertEqual(p['base_branch'], 'trunk')
        self.assertEqual(p['source'], {'path': str(self.repo.resolve()), 'owner': 'developer'})
        self.assertEqual([(c['name'], c['command']) for c in p['checks']],
                         [('unit', 'python3 -m unittest -q'), ('lint', 'ruff check .')])
        self.assertFalse(p['publish']['remote_push'])
        self.assertEqual(p['orchestrator']['merge'], 'none')
        self.assertTrue((self.etc / 'projects/demo/Dockerfile').read_text().startswith('ARG BASE\nFROM ${BASE}'))

    def test_refusals(self):
        with mock.patch('builtins.print'):
            self.add()
        for kw in ({}, {'id': 'Bad_id'}, {'owner': 'root'}, {'id': 'x2', 'harness': 'claude-code'},
                   {'id': 'x3', 'check': ['no-equals-sign']}, {'id': 'x4', 'path': self.tmp.name}):
            with self.assertRaises(SystemExit, msg=kw):
                self.add(**kw)

    def test_submit_refuses_uninstalled_harness(self):
        with mock.patch('builtins.print'):
            self.add(harness='claude-code', model='claude-test')
        self.write_settings('[prices]\n"claude-test" = { input = 1.0, output = 1.0, cache_read = 0.1, cache_write = 1.0 }\n')
        args = argparse.Namespace(project='demo', goal='Make a useful example change', accept=['The example works'],
                                  title=None, harness='claude-code', model='claude-test', mock='good', key=None,
                                  quiet=True, retry_of=None, role=None, json=True)
        with self.assertRaisesRegex(SystemExit, 'claude_binary'):
            cli.cmd_submit(args, self.conn)


if __name__ == '__main__':
    unittest.main()

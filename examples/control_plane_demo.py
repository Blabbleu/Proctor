#!/usr/bin/env python3
"""Run a credential-free, temporary control-plane exercise; no worker is started."""
import argparse
import json
import os
from pathlib import Path
import pwd
import shutil
import sqlite3
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='proctor-demo-') as tmp:
        root = Path(tmp)
        etc, state = root / 'etc', root / 'state'
        shutil.copytree(ROOT / 'config', etc)
        state.mkdir()
        os.environ['PROCTOR_ETC'] = str(etc)
        os.environ['PROCTOR_STATE'] = str(state)
        # Root cannot be a project owner. No repository operations are performed.
        owner = pwd.getpwuid(os.geteuid()).pw_name if os.geteuid() else 'developer'
        project_file = etc / 'projects/example/project.toml'
        project_file.write_text(project_file.read_text().replace('owner = "developer"', 'owner = ' + json.dumps(owner)))
        from proctor import cli, common, db, orch
        conn = sqlite3.connect(str(state / 'demo.db'), isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            db.init(conn)
            p = common.load_project('example')
            ns = argparse.Namespace(project='example', goal='Improve the example task intake flow', accept=['The result is recorded correctly'], title='Example task', harness='mock', model=None, mock='good', key='demo-key', quiet=True, retry_of=None, role=None, json=True)
            initial = orch.run_cli(cli.cmd_submit, ns, conn)
            duplicate = orch.run_cli(cli.cmd_submit, ns, conn)
            st = {'enabled_at': orch.clock() - 10, 'handled': {}, 'submitted': []}
            capacity = orch.capacity(conn, common.load_settings(), 'example', p, st, orch.BranchView(p), 'mock')
            count = conn.execute('SELECT count(*) FROM jobs').fetchone()[0]
            assert initial['state'] == 'queued'
            assert duplicate['duplicate'] and duplicate['id'] == initial['id']
            assert count == 1 and not capacity['ok'] and capacity['active'] == initial['id']
            result = {'initial': initial, 'duplicate': duplicate, 'capacity': capacity, 'job_count': count}
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                print('Queued:', initial['id'], initial['state'])
                print('Duplicate returned existing job:', duplicate['id'])
                print('Capacity refused:', '; '.join(capacity['reasons']))
                print('Jobs in temporary database:', count)
                print('No worker, container, gateway, or model was run.')
        finally:
            conn.close()


if __name__ == '__main__':
    main()

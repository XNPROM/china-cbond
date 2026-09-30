"""Exercise automatic publication against local Git repositories; no network."""
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
DATE = '2026-09-28'


def git(repo, *args):
    return subprocess.run(['git', '-C', str(repo), *args], check=True,
                          capture_output=True, text=True).stdout.strip()


@pytest.fixture
def publisher(tmp_path):
    repo = tmp_path / 'repo'
    repo.mkdir()
    (repo / 'scripts').mkdir()
    for name in ('auto_daily.sh', '_run_locked.py'):
        shutil.copy2(ROOT / 'scripts' / name, repo / 'scripts' / name)
    git(repo, 'init')
    git(repo, 'config', 'user.email', 'tests@example.invalid')
    git(repo, 'config', 'user.name', 'Test')
    (repo / '.gitignore').write_text('data/\n')
    git(repo, 'add', '.')
    git(repo, 'commit', '-m', 'initial')
    remote = tmp_path / 'origin.git'
    subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
    git(repo, 'remote', 'add', 'origin', str(remote))
    fake = tmp_path / 'fake-python'
    fake.write_text(f'#!{sys.executable}\n' + r'''
import json, os, pathlib, sys
args = sys.argv[1:]
if args[0].endswith('_run_locked.py'):
    os.execv(sys.executable, [sys.executable, *args])
repo = pathlib.Path.cwd()
with (repo / 'data' / 'calls.jsonl').open('a') as f:
    f.write(json.dumps(args) + '\n')
scenario = os.environ.get('SCENARIO', 'ok')
if args[0].endswith('daily_refresh.py'):
    count_file = repo / 'data' / 'attempts'
    n = int(count_file.read_text()) + 1 if count_file.exists() else 1
    count_file.write_text(str(n))
    if scenario == 'fail' or (scenario == 'retry' and n == 1):
        sys.exit(1)
    report = repo / 'reports' / args[args.index('--trade-date') + 1]
    report.mkdir(parents=True, exist_ok=True)
    for name in ('cbond_overview.md', 'cbond_overview.html', 'index.html'):
        (report / name).write_text('validated test report')
if args[0].endswith('validate_snapshot.py'):
    assert '--strict' in args and '--backtest' in args
    if scenario == 'bad_gate':
        sys.exit(1)
''')
    fake.chmod(0o755)
    env = dict(os.environ, CBOND_PYTHON=str(fake), AUTO_DAILY_ATTEMPTS='2', AUTO_DAILY_WAIT='0')
    def run(scenario='ok'):
        return subprocess.run(['/bin/bash', str(repo / 'scripts/auto_daily.sh'), DATE],
                              env=dict(env, SCENARIO=scenario), capture_output=True,
                              text=True, timeout=25)
    return repo, remote, run


@pytest.mark.parametrize('scenario,passes', [('ok', True), ('retry', True), ('fail', False), ('bad_gate', False)])
def test_publication_gates(publisher, scenario, passes):
    repo, remote, run = publisher
    before = git(repo, 'rev-parse', 'HEAD')
    result = run(scenario)
    assert (result.returncode == 0) is passes, result.stderr
    receipt = repo / 'data/logs' / f'published_{DATE}.commit'
    assert receipt.exists() is passes
    if passes:
        assert git(repo, 'rev-parse', 'HEAD') == git(remote, 'rev-parse', 'main')
        assert receipt.read_text().strip() == git(repo, 'rev-parse', 'HEAD')
    else:
        assert git(repo, 'rev-parse', 'HEAD') == before
        assert git(repo, 'diff', '--cached', '--name-only') == ''


def test_preserves_users_staged_changes(publisher):
    repo, _, run = publisher
    (repo / 'user.txt').write_text('unrelated work')
    git(repo, 'add', 'user.txt')
    before = git(repo, 'rev-parse', 'HEAD')
    assert run().returncode == 1
    assert git(repo, 'rev-parse', 'HEAD') == before
    assert git(repo, 'diff', '--cached', '--name-only') == 'user.txt'


def test_failed_push_is_retried_without_new_report_changes(publisher):
    repo, remote, run = publisher
    git(repo, 'remote', 'set-url', 'origin', str(repo / 'missing-remote'))
    assert run().returncode == 1
    head = git(repo, 'rev-parse', 'HEAD')
    assert not (repo / 'data/logs' / f'published_{DATE}.commit').exists()
    git(repo, 'remote', 'set-url', 'origin', str(remote))
    assert run().returncode == 0
    assert git(repo, 'rev-parse', 'HEAD') == head
    assert git(remote, 'rev-parse', 'main') == head

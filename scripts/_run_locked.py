"""Run the automatic publisher under a kernel lock, released even after a crash."""
import fcntl
import os
from pathlib import Path
import subprocess
import sys


def main():
    path, *command = sys.argv[1:]
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'a+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('[skip] another auto_daily instance is running')
            return 0
        # Inherit the lock descriptor so an orphaned child still holds the lock.
        env = dict(os.environ, CBOND_AUTO_LOCK_HELD='1')
        return subprocess.run(command, env=env, pass_fds=(lock.fileno(),)).returncode


if __name__ == '__main__':
    raise SystemExit(main())

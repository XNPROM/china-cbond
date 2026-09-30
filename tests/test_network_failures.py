import fcntl
import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import _ifind


def test_http_200_with_api_error_fails_instead_of_empty_market_data(monkeypatch):
    class Response:
        def raise_for_status(self): pass
        def json(self): return {'errorcode':-1,'errmsg':'quota exhausted'}
    class Session:
        def post(self,*a,**k): return Response()
    monkeypatch.setattr(_ifind,'get_access_token',lambda:'fixture')
    monkeypatch.setattr(_ifind,'_configure_session',lambda s:Session())
    monkeypatch.setattr(_ifind,'_ROUTE_REPORTED',True)
    with pytest.raises(RuntimeError,match='quota exhausted'):
        _ifind._post('basic_data_service',{},retries=1)


def test_kernel_lock_handles_contention_and_old_lock_file(tmp_path):
    wrapper=Path(__file__).resolve().parents[1]/'scripts/_run_locked.py'
    lock=tmp_path/'lock'
    marker=tmp_path/'marker'
    cmd=[sys.executable,str(wrapper),str(lock),sys.executable,'-c',f'from pathlib import Path; Path({str(marker)!r}).touch()']
    with lock.open('w') as handle:
        fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        assert subprocess.run(cmd,capture_output=True).returncode==0
        assert not marker.exists()
    # The file remains, but no live process owns the lock.
    assert subprocess.run(cmd,capture_output=True).returncode==0
    assert marker.exists()

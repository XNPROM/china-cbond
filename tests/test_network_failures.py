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


@pytest.mark.parametrize('status,expected', [(302, 0), (404, 0), (503, 1)])
def test_network_probe_uses_configured_session_without_credentials(monkeypatch, status, expected):
    import _network
    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def head(self, url, **kwargs):
            assert url == 'https://quantapi.51ifind.com/'
            assert 'headers' not in kwargs and kwargs['allow_redirects'] is False
            return type('Response', (), {'status_code': status})()
    monkeypatch.setattr(_network, 'configure_session', lambda s: Session())
    assert _network.probe_network() == expected


def test_network_probe_does_not_log_proxy_credentials(monkeypatch, capsys):
    import _network
    import requests
    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def head(self, *args, **kwargs):
            raise requests.ConnectionError('Failed to resolve https://user:secret@proxy.invalid')
    monkeypatch.setattr(_network, 'configure_session', lambda s: Session())
    assert _network.probe_network() == 1
    output = capsys.readouterr().out
    assert 'DNS resolution' in output and 'secret' not in output


def test_monthly_quota_failure_is_permanent_without_retries(monkeypatch):
    calls=[]
    class Response:
        def raise_for_status(self): pass
        def json(self): return {'errorcode':-4318,'errmsg':'quota exhausted'}
    class Session:
        def post(self,*a,**k):calls.append(1);return Response()
    monkeypatch.setattr(_ifind,'get_access_token',lambda:'fixture')
    monkeypatch.setattr(_ifind,'_configure_session',lambda s:Session())
    monkeypatch.setattr(_ifind,'_ROUTE_REPORTED',True)
    with pytest.raises(RuntimeError,match='monthly data-pool quota exhausted'):
        _ifind._post('data_pool',{},retries=3)
    assert len(calls)==1

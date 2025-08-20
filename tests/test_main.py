import subprocess
import pytest
import main

def test_run_script_success(monkeypatch):
    called = {}
    def fake_run(cmd, check, env):
        called["cmd"] = cmd
    monkeypatch.setattr(subprocess, "run", fake_run)
    main.run_script("dummy.py")
    assert called["cmd"][:2] == ["python", "dummy.py"]

def test_run_script_failure(monkeypatch):
    def fake_run(cmd, check, env):
        raise subprocess.CalledProcessError(returncode=1, cmd=cmd)
    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(SystemExit):
        main.run_script("bad.py")

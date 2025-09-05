import os
import sys
from pathlib import Path
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import scheduled_input_engine.ScheduledInputBackend as sib


class DummyDF:
    def __init__(self, recorder):
        self._recorder = recorder

    def to_parquet(self, path, index=False, compression='gzip'):
        # Record the path where data would be saved
        self._recorder['path'] = Path(path)


class FakeExecution:
    def __init__(self, output_filename):
        self.output_path = output_filename

    def execute_code(self, *_args, **_kwargs):
        return DummyDF(self._recorder)


@pytest.fixture
def backend(monkeypatch, tmp_path):
    recorder = {}

    def _make_backend(output_filename):
        exec_instance = FakeExecution(output_filename)
        exec_instance._recorder = recorder

        monkeypatch.setattr(sib, 'SIExecution', lambda code: exec_instance)
        monkeypatch.setattr(sib, 'get_cached_or_fetch', lambda *a, **k: None)

        backend_obj = sib.ScheduledInputBackend.__new__(sib.ScheduledInputBackend)
        backend_obj.INDEXES_DIR = tmp_path
        return backend_obj, recorder

    return _make_backend


def test_execute_scheduled_code_rejects_outside_path(backend):
    backend_obj, _ = backend('../evil.system4.system4.parquet')
    with pytest.raises(ValueError):
        backend_obj.execute_scheduled_code('t', 'code', False, '')


def test_execute_scheduled_code_saves_within_target_dir(backend):
    backend_obj, recorder = backend('results.system4.system4.parquet')
    backend_obj.execute_scheduled_code('t', 'code', False, '')
    assert recorder['path'].resolve().parent == backend_obj.INDEXES_DIR.resolve()

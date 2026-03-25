"""Tests for view.py utility functions.

Like run.py, view.py has module-level Streamlit code that can't be imported
in a test context. We re-implement the pure utility functions here for testing.
"""
import os
import pytest
import tempfile
import shutil
import datetime
from pathlib import Path
from unittest.mock import MagicMock
import time as _time


@pytest.fixture
def tmp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


# ── Re-implemented pure functions from view.py ──────────────────────────────

def list_folders(folder):
    ret = []
    for f in os.scandir(folder):
        if f.is_dir():
            ret.append([f.name, datetime.datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')])
    ret.sort(key=lambda x: x[1], reverse=True)
    return ret


def save_file(file, folder) -> str:
    if file:
        dest = Path(folder) / file.name
        dest.write_bytes(file.read())
        return os.path.join(folder, dest.name)
    return None


def store_value(key, session_state):
    session_state[key] = session_state["_" + key]


def load_value(key, session_state):
    if key in session_state:
        session_state["_" + key] = session_state[key]


def clear_value(key, session_state):
    if key in session_state:
        session_state.pop(key)
        session_state.pop("_" + key)


# ── list_folders ─────────────────────────────────────────────────────────────

class TestListFolders:

    def test_empty_data_dir(self, tmp_dir):
        assert list_folders(tmp_dir) == []

    def test_returns_dirs_only(self, tmp_dir):
        os.mkdir(os.path.join(tmp_dir, "test1"))
        Path(os.path.join(tmp_dir, "file.txt")).write_text("x")
        result = list_folders(tmp_dir)
        assert len(result) == 1
        assert result[0][0] == "test1"

    def test_sorted_by_mtime_desc(self, tmp_dir):
        os.mkdir(os.path.join(tmp_dir, "old_test"))
        _time.sleep(1.1)  # need >1s gap since sort is by second-precision timestamp string
        os.mkdir(os.path.join(tmp_dir, "new_test"))
        result = list_folders(tmp_dir)
        assert result[0][0] == "new_test"
        assert result[1][0] == "old_test"

    def test_returns_formatted_timestamp(self, tmp_dir):
        os.mkdir(os.path.join(tmp_dir, "test1"))
        result = list_folders(tmp_dir)
        ts = result[0][1]
        datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")  # should not raise

    def test_multiple_folders(self, tmp_dir):
        for name in ["a", "b", "c"]:
            os.mkdir(os.path.join(tmp_dir, name))
        result = list_folders(tmp_dir)
        assert len(result) == 3

    def test_ignores_files(self, tmp_dir):
        Path(os.path.join(tmp_dir, "not_a_dir.txt")).write_text("x")
        Path(os.path.join(tmp_dir, "also_not.csv")).write_text("y")
        assert list_folders(tmp_dir) == []


# ── save_file ────────────────────────────────────────────────────────────────

class TestViewSaveFile:

    def test_save_file_returns_path(self, tmp_dir):
        mock_file = MagicMock()
        mock_file.name = "data.zip"
        mock_file.read.return_value = b"zipdata"
        result = save_file(mock_file, tmp_dir)
        assert result == os.path.join(tmp_dir, "data.zip")
        assert os.path.exists(result)

    def test_save_file_none_returns_none(self, tmp_dir):
        assert save_file(None, tmp_dir) is None

    def test_save_file_content_correct(self, tmp_dir):
        mock_file = MagicMock()
        mock_file.name = "test.csv"
        mock_file.read.return_value = b"col1,col2\n1,2"
        result = save_file(mock_file, tmp_dir)
        assert Path(result).read_bytes() == b"col1,col2\n1,2"


# ── session state helpers ────────────────────────────────────────────────────

class TestViewSessionHelpers:

    def test_store_and_load(self):
        state = {"_k": "v"}
        store_value("k", state)
        assert state["k"] == "v"
        state["k"] = "new"
        load_value("k", state)
        assert state["_k"] == "new"

    def test_clear_value(self):
        state = {"k": 1, "_k": 2}
        clear_value("k", state)
        assert "k" not in state
        assert "_k" not in state

    def test_load_missing_key(self):
        state = {}
        load_value("nope", state)
        assert "_nope" not in state

    def test_clear_missing_key_is_noop(self):
        state = {"other": 1}
        clear_value("missing", state)
        assert state == {"other": 1}

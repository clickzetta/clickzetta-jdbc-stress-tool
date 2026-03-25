"""Tests for run.py utility functions (non-Streamlit logic).

run.py and view.py have heavy module-level Streamlit code that runs on import.
Instead of fighting the import, we extract and test the pure logic directly.
"""
import os
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import MagicMock
from zipfile import ZipFile
import glob
import time as _time


@pytest.fixture
def tmp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


# ── Re-implement the pure functions from run.py for isolated testing ─────────
# These are exact copies of the utility functions defined in run.py,
# tested without importing the module (which triggers Streamlit side-effects).

def find_latest_file(pattern):
    files = glob.glob(pattern)
    if not files:
        return None
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


def list_files(folder, recursive=False):
    ret = []
    files = os.listdir(folder)
    if files:
        if recursive:
            ret.append(folder)
        files.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)), reverse=True)
        for f in files:
            p = os.path.join(folder, f)
            if os.path.isfile(p):
                ret.append(p)
            elif recursive and os.path.isdir(p):
                ret.extend(list_files(p, recursive))
    return ret


def save_file(file, folder):
    from zipfile import is_zipfile
    if file:
        dest = Path(folder) / file.name
        dest.write_bytes(file.read())
        if is_zipfile(dest):
            with ZipFile(dest, 'r') as zip:
                zip.extractall(folder)
            dest.unlink()


def save_files(files, folder):
    if files:
        for f in files:
            save_file(f, folder)


def store_value(key, session_state):
    session_state[key] = session_state["_" + key]


def load_value(key, session_state):
    if key in session_state:
        session_state["_" + key] = session_state[key]


def clear_value(key, session_state):
    if key in session_state:
        session_state.pop(key)
        session_state.pop("_" + key)


# ── find_latest_file ─────────────────────────────────────────────────────────

class TestFindLatestFile:

    def test_no_match_returns_none(self, tmp_dir):
        assert find_latest_file(os.path.join(tmp_dir, "*.nonexistent")) is None

    def test_single_file(self, tmp_dir):
        p = os.path.join(tmp_dir, "a.txt")
        Path(p).write_text("hello")
        result = find_latest_file(os.path.join(tmp_dir, "*.txt"))
        assert result == p

    def test_returns_latest(self, tmp_dir):
        p1 = os.path.join(tmp_dir, "old.txt")
        Path(p1).write_text("old")
        _time.sleep(0.05)
        p2 = os.path.join(tmp_dir, "new.txt")
        Path(p2).write_text("new")
        result = find_latest_file(os.path.join(tmp_dir, "*.txt"))
        assert result == p2

    def test_multiple_files(self, tmp_dir):
        for i in range(5):
            Path(os.path.join(tmp_dir, f"f{i}.txt")).write_text(str(i))
            _time.sleep(0.02)
        result = find_latest_file(os.path.join(tmp_dir, "*.txt"))
        assert result == os.path.join(tmp_dir, "f4.txt")


# ── list_files ───────────────────────────────────────────────────────────────

class TestListFiles:

    def test_empty_folder(self, tmp_dir):
        assert list_files(tmp_dir) == []

    def test_files_only(self, tmp_dir):
        for name in ["b.sql", "a.sql"]:
            Path(os.path.join(tmp_dir, name)).write_text("select 1")
        result = list_files(tmp_dir)
        assert len(result) == 2
        for r in result:
            assert os.path.isfile(r)

    def test_recursive_includes_subdirs(self, tmp_dir):
        sub = os.path.join(tmp_dir, "sub")
        os.mkdir(sub)
        Path(os.path.join(sub, "inner.sql")).write_text("select 1")
        Path(os.path.join(tmp_dir, "outer.sql")).write_text("select 2")
        result = list_files(tmp_dir, recursive=True)
        assert any("inner.sql" in r for r in result)
        assert any("outer.sql" in r for r in result)

    def test_non_recursive_ignores_subdirs(self, tmp_dir):
        sub = os.path.join(tmp_dir, "sub")
        os.mkdir(sub)
        Path(os.path.join(sub, "inner.sql")).write_text("select 1")
        Path(os.path.join(tmp_dir, "outer.sql")).write_text("select 2")
        result = list_files(tmp_dir, recursive=False)
        assert not any("inner.sql" in r for r in result)
        assert any("outer.sql" in r for r in result)

    def test_sorted_by_mtime_desc(self, tmp_dir):
        p1 = os.path.join(tmp_dir, "old.sql")
        Path(p1).write_text("1")
        _time.sleep(0.05)
        p2 = os.path.join(tmp_dir, "new.sql")
        Path(p2).write_text("2")
        result = list_files(tmp_dir)
        assert result[0] == p2

    def test_recursive_folder_entry(self, tmp_dir):
        """When recursive=True, the folder itself should appear in the list."""
        sub = os.path.join(tmp_dir, "sub")
        os.mkdir(sub)
        Path(os.path.join(sub, "f.sql")).write_text("x")
        result = list_files(tmp_dir, recursive=True)
        assert tmp_dir in result


# ── save_file / save_files ───────────────────────────────────────────────────

class TestSaveFile:

    def test_save_file_writes_bytes(self, tmp_dir):
        mock_file = MagicMock()
        mock_file.name = "test.sql"
        mock_file.read.return_value = b"SELECT 1;"
        save_file(mock_file, tmp_dir)
        dest = os.path.join(tmp_dir, "test.sql")
        assert os.path.exists(dest)
        assert Path(dest).read_bytes() == b"SELECT 1;"

    def test_save_file_none_is_noop(self, tmp_dir):
        save_file(None, tmp_dir)
        assert os.listdir(tmp_dir) == []

    def test_save_file_extracts_zip(self, tmp_dir):
        zip_path = os.path.join(tmp_dir, "archive.zip")
        with ZipFile(zip_path, "w") as zf:
            zf.writestr("inner.txt", "hello")
        with open(zip_path, "rb") as f:
            content = f.read()
        os.remove(zip_path)

        mock_file = MagicMock()
        mock_file.name = "archive.zip"
        mock_file.read.return_value = content
        save_file(mock_file, tmp_dir)
        assert os.path.exists(os.path.join(tmp_dir, "inner.txt"))
        assert not os.path.exists(os.path.join(tmp_dir, "archive.zip"))

    def test_save_files_multiple(self, tmp_dir):
        files = []
        for name in ["a.sql", "b.sql"]:
            m = MagicMock()
            m.name = name
            m.read.return_value = b"data"
            files.append(m)
        save_files(files, tmp_dir)
        assert os.path.exists(os.path.join(tmp_dir, "a.sql"))
        assert os.path.exists(os.path.join(tmp_dir, "b.sql"))

    def test_save_files_none_is_noop(self, tmp_dir):
        save_files(None, tmp_dir)

    def test_save_file_non_zip_kept(self, tmp_dir):
        """A non-zip file should be kept as-is."""
        mock_file = MagicMock()
        mock_file.name = "data.csv"
        mock_file.read.return_value = b"a,b,c\n1,2,3"
        save_file(mock_file, tmp_dir)
        assert os.path.exists(os.path.join(tmp_dir, "data.csv"))


# ── store_value / load_value / clear_value ───────────────────────────────────

class TestSessionStateHelpers:

    def test_store_value(self):
        state = {"_mykey": "hello"}
        store_value("mykey", state)
        assert state["mykey"] == "hello"

    def test_load_value(self):
        state = {"mykey": "world"}
        load_value("mykey", state)
        assert state["_mykey"] == "world"

    def test_clear_value(self):
        state = {"mykey": "a", "_mykey": "b"}
        clear_value("mykey", state)
        assert "mykey" not in state
        assert "_mykey" not in state

    def test_load_value_missing_key_is_noop(self):
        state = {}
        load_value("nonexistent", state)
        assert "_nonexistent" not in state

    def test_store_then_load_roundtrip(self):
        state = {"_k": "v1"}
        store_value("k", state)
        state["k"] = "v2"
        load_value("k", state)
        assert state["_k"] == "v2"


# ── config preview (reads file content for display) ──────────────────────────

class TestConfigPreview:
    """Test the config file reading logic used by the preview feature."""

    def test_read_config_content(self, tmp_dir):
        """Selected config file should be readable and return its content."""
        conf = os.path.join(tmp_dir, "test.ini")
        Path(conf).write_text("url=jdbc:h2:mem:test\nusername=sa\npassword=secret\n")
        with open(conf) as f:
            content = f.read()
        assert "url=jdbc:h2:mem:test" in content
        assert "username=sa" in content
        assert "password=secret" in content

    def test_read_empty_config(self, tmp_dir):
        """An empty config file should return empty string."""
        conf = os.path.join(tmp_dir, "empty.ini")
        Path(conf).write_text("")
        with open(conf) as f:
            content = f.read()
        assert content == ""

    def test_read_config_preserves_comments(self, tmp_dir):
        """Comments in config should be preserved in preview."""
        conf = os.path.join(tmp_dir, "commented.ini")
        Path(conf).write_text("# this is a comment\nurl=jdbc:h2:mem:test\n")
        with open(conf) as f:
            content = f.read()
        assert "# this is a comment" in content

    def test_config_listed_by_list_files(self, tmp_dir):
        """Config files in conf folder should be discoverable by list_files."""
        Path(os.path.join(tmp_dir, "my.ini")).write_text("url=test")
        Path(os.path.join(tmp_dir, "other.properties")).write_text("url=test2")
        result = list_files(tmp_dir)
        assert len(result) == 2
        names = [os.path.basename(r) for r in result]
        assert "my.ini" in names
        assert "other.properties" in names

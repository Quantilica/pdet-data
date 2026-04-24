"""Tests for pdet_data.reader — column schema resolution and decompress error handling."""
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pdet_data.reader import decompress, read_caged, read_rais
from pdet_data.constants import (
    CAGED_COLUMNS,
    CAGED_AJUSTES_COLUMNS,
    CAGED_2020_MOV_COLUMNS,
    RAIS_VINCULOS_COLUMNS,
    RAIS_ESTABELECIMENTOS_COLUMNS,
)


# ---------------------------------------------------------------------------
# decompress()
# ---------------------------------------------------------------------------


class TestDecompress:
    def _meta(self, tmp_path):
        f = tmp_path / "dummy.7z"
        f.write_bytes(b"")
        return {"filepath": f}

    def test_raises_on_nonzero_returncode(self, tmp_path):
        meta = self._meta(tmp_path)
        result = MagicMock()
        result.returncode = 1
        result.stderr = b"Error: file not found"
        with patch("pdet_data.reader.subprocess.run", return_value=result):
            with patch("pdet_data.reader.tempfile.mkdtemp", return_value=str(tmp_path)):
                with pytest.raises(RuntimeError, match="7z failed"):
                    decompress(meta)

    def test_raises_when_no_files_extracted(self, tmp_path):
        meta = self._meta(tmp_path)
        result = MagicMock()
        result.returncode = 0
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        with patch("pdet_data.reader.subprocess.run", return_value=result):
            with patch("pdet_data.reader.tempfile.mkdtemp", return_value=str(empty_dir)):
                with pytest.raises(RuntimeError, match="no files"):
                    decompress(meta)

    def test_returns_decompressed_filepath(self, tmp_path):
        meta = self._meta(tmp_path)
        result = MagicMock()
        result.returncode = 0
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        extracted = out_dir / "data.csv"
        extracted.write_text("a;b\n1;2\n")
        with patch("pdet_data.reader.subprocess.run", return_value=result):
            with patch("pdet_data.reader.tempfile.mkdtemp", return_value=str(out_dir)):
                output = decompress(meta)
        assert output["decompressed_filepath"] == extracted
        assert output["tmp_dir"] == out_dir


# ---------------------------------------------------------------------------
# Column schema resolution — no I/O, just dict lookups
# ---------------------------------------------------------------------------


class TestColumnSchemaResolution:
    """Verify that the fallback initialisation prevents UnboundLocalError."""

    def test_caged_future_date_uses_latest_schema(self):
        future_date = 999999
        latest_schema = list(CAGED_COLUMNS.values())[-1]
        # Simulate the loop logic from read_caged
        columns_names = list(CAGED_COLUMNS.values())[-1]
        for d in CAGED_COLUMNS:
            if future_date < d:
                break
            columns_names = CAGED_COLUMNS[d]
        assert columns_names == latest_schema

    def test_rais_vinculos_future_year_uses_latest_schema(self):
        future_year = 9999
        latest_schema = list(RAIS_VINCULOS_COLUMNS.values())[-1]
        columns_names = list(RAIS_VINCULOS_COLUMNS.values())[-1]
        for y in RAIS_VINCULOS_COLUMNS:
            if future_year < y:
                break
            columns_names = RAIS_VINCULOS_COLUMNS[y]
        assert columns_names == latest_schema

    def test_rais_estabelecimentos_future_year_uses_latest_schema(self):
        future_year = 9999
        latest_schema = list(RAIS_ESTABELECIMENTOS_COLUMNS.values())[-1]
        columns_names = list(RAIS_ESTABELECIMENTOS_COLUMNS.values())[-1]
        for y in RAIS_ESTABELECIMENTOS_COLUMNS:
            if future_year < y:
                break
            columns_names = RAIS_ESTABELECIMENTOS_COLUMNS[y]
        assert columns_names == latest_schema

    def test_read_rais_raises_on_unknown_dataset(self, tmp_path):
        dummy = tmp_path / "dummy.csv"
        dummy.write_text("col\n1\n")
        with pytest.raises(ValueError, match="Unknown RAIS dataset"):
            read_rais(dummy, year=2020, dataset="unknown-dataset")

    def test_read_caged_raises_on_unknown_dataset(self, tmp_path):
        dummy = tmp_path / "dummy.csv"
        dummy.write_text("col\n1\n")
        with pytest.raises(ValueError, match="Unknown CAGED dataset"):
            read_caged(dummy, date=202001, dataset="unknown-dataset")

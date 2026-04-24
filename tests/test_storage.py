"""Tests for pdet_data.storage — pure functions, no I/O."""
from datetime import datetime

import pytest

from pdet_data.storage import (
    get_caged_filename,
    get_caged_2020_filename,
    get_rais_filename,
)


MOCK_DATETIME = datetime(2024, 3, 15)


def _caged_meta(**kwargs):
    return {"dataset": "caged", "datetime": MOCK_DATETIME, "extension": "parquet", **kwargs}


def _caged_2020_meta(**kwargs):
    return {"dataset": "caged-2020-mov", "datetime": MOCK_DATETIME, "extension": "parquet", **kwargs}


def _rais_meta(**kwargs):
    return {"dataset": "rais-vinculos", "datetime": MOCK_DATETIME, "extension": "parquet", **kwargs}


class TestGetCagedFilename:
    def test_monthly_partition(self):
        meta = _caged_meta(year=2022, month=1)
        result = get_caged_filename(meta)
        assert result == "caged_202201@20240315.parquet"

    def test_yearly_partition_no_month(self):
        meta = _caged_meta(year=2022, month=None)
        result = get_caged_filename(meta)
        assert result == "caged_2022@20240315.parquet"

    def test_includes_modification_date(self):
        meta = _caged_meta(year=2020, month=6)
        result = get_caged_filename(meta)
        assert "20240315" in result


class TestGetCaged2020Filename:
    def test_basic(self):
        meta = _caged_2020_meta(year=2022, month=1)
        result = get_caged_2020_filename(meta)
        assert result == "caged-2020-mov_202201@20240315.parquet"

    def test_extension_reflected(self):
        meta = _caged_2020_meta(year=2022, month=1, extension="7z")
        result = get_caged_2020_filename(meta)
        assert result.endswith(".7z")


class TestGetRaisFilename:
    def test_without_uf(self):
        meta = _rais_meta(year=2022)
        result = get_rais_filename(meta)
        assert result == "rais-vinculos_2022@20240315.parquet"

    def test_with_uf(self):
        meta = _rais_meta(year=2022, uf="SP")
        result = get_rais_filename(meta)
        assert result == "rais-vinculos_2022-SP@20240315.parquet"

    def test_with_region(self):
        meta = _rais_meta(year=2022, region="CO")
        result = get_rais_filename(meta)
        assert result == "rais-vinculos_2022-CO@20240315.parquet"

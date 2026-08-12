import logging
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import polars as pl

from .constants import (
    BOOLEAN_COLUMNS,
    CAGED_2020_EXC_COLUMNS,
    CAGED_2020_FOR_COLUMNS,
    CAGED_2020_MOV_COLUMNS,
    CAGED_AJUSTES_COLUMNS,
    CAGED_COLUMNS,
    INTEGER_COLUMNS,
    NA_VALUES,
    NUMERIC_COLUMNS,
    RAGGED_CSV_FILES,
    RAIS_ESTABELECIMENTOS_COLUMNS,
    RAIS_VINCULOS_COLUMNS,
)

logger = logging.getLogger(__name__)


def parse_filename(f: Path) -> dict[str, str | int | None]:
    """Parses a filename to extract metadata.

    Args:
        f (Path): The file path to parse.

    Returns:
        dict[str, str | int | None]: A dictionary containing the parsed metadata.
    """
    m = re.search(r"^([a-z0-9-]+)_([a-z0-9-]+)@(\d{8})\.(7z|zip)$", f.name)
    dataset, partition, modification, extension = m.groups()
    date_uf = partition.split("-")
    if len(date_uf) == 2:
        date, uf = date_uf
    else:
        date = date_uf[0]
        uf = None
    return {
        "filepath": f,
        "filename": f.name,
        "name": f.stem,
        "extension": extension,
        "modification": modification,
        "dataset": dataset,
        "date": int(date),
        "uf": uf,
    }


def convert_columns_dtypes(df: pl.DataFrame) -> pl.DataFrame:
    """Converts the datatypes of columns in a DataFrame based on predefined constants.

    Args:
        df (pl.DataFrame): The input DataFrame.

    Returns:
        pl.DataFrame: A DataFrame with the converted datatypes.
    """
    for column in df.columns:
        if column in INTEGER_COLUMNS:
            df = df.with_columns(
                pl.col(column)
                .str.replace(r" +", "")
                .str.replace(r"\.", "")
                .cast(pl.Int64)
            )
        elif column in NUMERIC_COLUMNS:
            df = df.with_columns(
                pl.col(column)
                .str.replace(r" +", "")
                .str.replace(r"\.", "")
                .str.replace(",", ".")
                .cast(pl.Float64)
            )
        elif column in BOOLEAN_COLUMNS:
            df = df.with_columns(pl.col(column).cast(pl.Int8).cast(pl.Boolean))
        else:  # Categorical
            df = df.with_columns(pl.col(column).str.strip_chars().cast(pl.Categorical))
    return df


def _fix_ragged_csv(filepath: Path, encoding: str) -> Path:
    import csv

    logger.info("Fixing ragged CSV file: %s", filepath)

    dest_filepath = filepath.with_suffix(".fixed.csv")
    with open(filepath, encoding=encoding) as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        header_len = len(header)
        with open(dest_filepath, "w", encoding=encoding, newline="\n") as ff:
            writer = csv.writer(ff, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            for row in reader:
                writer.writerow(row[:header_len])

    return dest_filepath


def _sniff_separator(filepath: Path, encoding: str) -> str:
    with open(filepath, encoding=encoding, errors="replace") as f:
        first_line = f.readline()
    counts = {sep: first_line.count(sep) for sep in (";", "\t", ",")}
    return max(counts, key=counts.__getitem__)


def _resolve_columns(schema_dict: dict[int, tuple], date_key: int) -> tuple:
    columns_names = list(schema_dict.values())[-1]
    for key in schema_dict:
        if date_key < key:
            break
        columns_names = schema_dict[key]
    return columns_names


def read_rais(filepath: Path, year: int, dataset: str, **read_csv_args) -> pl.DataFrame:
    """Reads a RAIS dataset from a CSV file into a Polars DataFrame.

    Args:
        filepath (Path): The path to the CSV file.
        year (int): The year of the dataset.
        dataset (str): The specific RAIS dataset to read ('vinculos' or 'estabelecimentos').
        **read_csv_args: Additional keyword arguments for polars.read_csv.

    Returns:
        pl.DataFrame: The loaded and typed DataFrame.

    Raises:
        ValueError: If an unknown RAIS dataset is provided.
    """
    if dataset == "vinculos":
        columns_names = _resolve_columns(RAIS_VINCULOS_COLUMNS, year)
    elif dataset == "estabelecimentos":
        columns_names = _resolve_columns(RAIS_ESTABELECIMENTOS_COLUMNS, year)
    else:
        raise ValueError(f"Unknown RAIS dataset: {dataset!r}")
    logger.info("Reading %s from %s", dataset, filepath)
    separator = _sniff_separator(filepath, encoding="latin1")
    df = pl.read_csv(
        filepath,
        has_header=True,
        new_columns=columns_names,
        separator=separator,
        encoding="latin1",
        null_values=NA_VALUES,
        infer_schema_length=0,
        **read_csv_args,
    )
    df = convert_columns_dtypes(df)
    return df


def read_caged(
    filepath: Path, date: int, dataset: str, **read_csv_args
) -> pl.DataFrame:
    """Reads a CAGED dataset from a CSV file into a Polars DataFrame.

    Args:
        filepath (Path): The path to the CSV file.
        date (int): The date (usually year-month format) of the dataset.
        dataset (str): The specific CAGED dataset to read.
        **read_csv_args: Additional keyword arguments for polars.read_csv.

    Returns:
        pl.DataFrame: The loaded and typed DataFrame.

    Raises:
        ValueError: If an unknown CAGED dataset is provided.
    """
    if dataset == "caged":
        encoding = "latin-1"
        columns_names = _resolve_columns(CAGED_COLUMNS, date)
    elif dataset == "caged-ajustes":
        encoding = "latin-1"
        columns_names = _resolve_columns(CAGED_AJUSTES_COLUMNS, date)
    elif dataset == "caged-2020-exc":
        encoding = "utf-8"
        columns_names = _resolve_columns(CAGED_2020_EXC_COLUMNS, date)
    elif dataset == "caged-2020-for":
        encoding = "utf-8"
        columns_names = _resolve_columns(CAGED_2020_FOR_COLUMNS, date)
    elif dataset == "caged-2020-mov":
        encoding = "utf-8"
        columns_names = _resolve_columns(CAGED_2020_MOV_COLUMNS, date)
    else:
        raise ValueError(f"Unknown CAGED dataset: {dataset!r}")

    logger.info("Reading %s from %s", dataset, filepath)
    if filepath.name in RAGGED_CSV_FILES:
        filepath = _fix_ragged_csv(filepath, encoding)
    df = pl.read_csv(
        filepath,
        has_header=True,
        new_columns=columns_names,
        separator=";",
        encoding=encoding,
        null_values=NA_VALUES,
        infer_schema_length=0,
        **read_csv_args,
    )
    df = convert_columns_dtypes(df)
    return df


def write_parquet(df: pl.DataFrame, filepath: Path) -> Path:
    """Writes a DataFrame to a Parquet file.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        filepath (Path): The destination file path.

    Returns:
        Path: The file path where the Parquet file was written.
    """
    logger.info("Writing data to %s", filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(filepath)
    return filepath


def decompress(file_metadata: dict[str, Any]) -> dict[str, Path]:
    """Decompresses an archive file using 7z.

    Args:
        file_metadata (dict[str, Any]): A dictionary containing metadata for the file, including 'filepath'.

    Returns:
        dict[str, Path]: The file metadata updated with paths to the temporary directory and decompressed file.

    Raises:
        RuntimeError: If 7z fails to decompress or produces no output files.
    """
    compressed_filepath = file_metadata["filepath"]
    logger.info("Decompressing %s", compressed_filepath)
    tmp_dir = Path(tempfile.mkdtemp(prefix="pdet"))
    command = [
        "7z",
        "e",
        str(compressed_filepath),
        f"-o{tmp_dir}",
    ]
    result = subprocess.run(
        command,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"7z failed decompressing {compressed_filepath}: "
            f"{result.stderr.decode(errors='replace')}"
        )
    extracted = list(tmp_dir.iterdir())
    if not extracted:
        raise RuntimeError(f"7z produced no files from {compressed_filepath}")
    decompressed_filepath = extracted[0]
    return file_metadata | {
        "tmp_dir": tmp_dir,
        "decompressed_filepath": decompressed_filepath,
    }

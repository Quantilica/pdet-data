import argparse
import csv
import shutil
from pathlib import Path

import polars as pl

from pdet_data import reader

DIR = Path(__file__).parent.absolute()


def get_columns(filepath: Path, encoding: str = "latin-1") -> list[str]:
    df: pl.DataFrame = pl.read_csv(
        filepath,
        n_rows=1,
        encoding=encoding,
        separator=";",
        has_header=True,
        infer_schema_length=0,
    )
    return df.columns


def extract_columns_for_dataset(
    data_dir: Path, glob_pattern: str, output_file: Path, encoding: str = "latin-1", has_uf: bool = False
) -> None:
    fieldnames = ["column", "order", "name", "date"]
    if has_uf:
        fieldnames.append("uf")

    with open(output_file, "w", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for file in data_dir.rglob(glob_pattern):
            print(file)
            file_metadata = reader.parse_filename(file)
            file_metadata = reader.decompress(file_metadata)
            columns = get_columns(file_metadata["decompressed_filepath"], encoding=encoding)
            shutil.rmtree(file_metadata["tmp_dir"])
            print(file)
            print(columns)
            for order, column in enumerate(columns):
                row = {
                    "column": column,
                    "order": order,
                    "name": file_metadata["name"],
                    "date": file_metadata["date"],
                }
                if has_uf:
                    row["uf"] = file_metadata["uf"]
                writer.writerow(row)


DATASETS = {
    "rais-estabelecimentos": {
        "glob_pattern": "rais-*.*",
        "output_file": "rais-estabelecimentos-columns.csv",
        "has_uf": True,
        "encoding": "latin-1",
    },
    "rais-vinculos": {
        "glob_pattern": "rais-*.*",
        "output_file": "rais-vinculos-columns.csv",
        "has_uf": True,
        "encoding": "latin-1",
    },
    "caged": {
        "glob_pattern": "caged_*.*",
        "output_file": "caged-columns.csv",
        "has_uf": False,
        "encoding": "latin-1",
    },
    "caged-ajustes": {
        "glob_pattern": "caged-ajustes_*.*",
        "output_file": "caged-ajustes-columns.csv",
        "has_uf": False,
        "encoding": "latin-1",
    },
    "caged-2020": {
        "glob_pattern": "caged-2020-*.*",
        "output_file": "caged-2020-columns.csv",
        "has_uf": False,
        "encoding": "utf-8",
    },
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-data-dir", type=Path)
    parser.add_argument("dataset")
    args = parser.parse_args()
    data_dir = args.data_dir
    dataset = args.dataset

    if dataset not in DATASETS:
        raise ValueError(f"Unknown dataset: {dataset}")

    config = DATASETS[dataset]
    output_file = DIR / config["output_file"]
    extract_columns_for_dataset(
        data_dir,
        config["glob_pattern"],
        output_file,
        encoding=config["encoding"],
        has_uf=config["has_uf"],
    )


if __name__ == "__main__":
    main()

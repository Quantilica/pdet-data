import argparse
import shutil
from pathlib import Path

import polars as pl

from pdet_data import reader

DIR = Path(__file__).parent.absolute()


def convert_caged(filepath: Path, dest_dir: Path):
    file_metadata = reader.parse_filename(filepath)
    date = file_metadata["date"]
    name = file_metadata["name"]
    dataset = file_metadata["dataset"]

    dest_filepath = dest_dir / dataset / f"{name}.parquet"
    if dest_filepath.exists():
        return

    decompressed = reader.decompress(file_metadata)
    decompressed_filepath = decompressed["decompressed_filepath"]

    try:
        df = reader.read_caged(
            decompressed_filepath,
            date=date,
            dataset=dataset,
        )
        reader.write_parquet(df, dest_filepath)
    except pl.exceptions.ComputeError as e:
        print(f"Error converting {decompressed_filepath}: {e}")
    except pl.exceptions.ShapeError as e:
        print(f"Error converting {decompressed_filepath}: {e}")
    finally:
        shutil.rmtree(decompressed["tmp_dir"])

    print(f"Done {filepath}")


def main():
    parser = argparse.ArgumentParser(description="Decompress CAGED files")
    parser.add_argument("-data-dir", type=Path, help="Path to the data directory")
    parser.add_argument(
        "-dest-dir", type=Path, help="Path to the destination directory"
    )
    args = parser.parse_args()
    data_dir = args.data_dir
    dest_dir = args.dest_dir

    latest_files: dict[tuple, dict] = {}
    for file in data_dir.glob("**/caged*.*"):
        if file.suffix not in (".zip", ".7z"):
            continue
        file_metadata = reader.parse_filename(file)
        key = (file_metadata["dataset"], file_metadata["date"], file_metadata["uf"])
        if key not in latest_files or file_metadata["modification"] > latest_files[key]["modification"]:
            latest_files[key] = file_metadata

    for file_metadata in latest_files.values():
        print(file_metadata["filepath"])
        convert_caged(file_metadata["filepath"], dest_dir)


if __name__ == "__main__":
    main()

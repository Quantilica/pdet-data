import argparse
import shutil
from pathlib import Path

import polars as pl

from pdet_data import reader

DIR = Path(__file__).parent.absolute()


def convert_rais_directory(data_raw_rais_year_dir: Path, dataset: str, dest_intermediary_rais_dir: Path):
    year = int(data_raw_rais_year_dir.name)
    print(f"Converting RAIS {dataset} data for year {year}...")
    all_files: dict[str | None, dict] = {}
    for file in data_raw_rais_year_dir.iterdir():
        file_metadata = reader.parse_filename(file)
        uf = file_metadata["uf"]
        if uf not in all_files or file_metadata["modification"] > all_files[uf]["modification"]:
            all_files[uf] = file_metadata
    files = list(all_files.values())

    latest_modification = max(file_metadata["modification"] for file_metadata in files)

    dest_filepath = dest_intermediary_rais_dir / f"{dataset}_{year}@{latest_modification}.parquet"
    if dest_filepath.exists():
        return

    with pl.StringCache():
        frames = []
        for file_metadata in files:
            decompressed = reader.decompress(file_metadata)
            decompressed_filepath = decompressed["decompressed_filepath"]
            df = reader.read_rais(
                decompressed_filepath,
                year=year,
                dataset=dataset,
            )
            frames.append(df)
            shutil.rmtree(decompressed["tmp_dir"])
        data = pl.concat(frames, how="vertical")

    reader.write_parquet(data, dest_filepath)


def main():
    parser = argparse.ArgumentParser(description="Decompress RAIS files")
    parser.add_argument("-data-dir", type=Path, help="Path to the data directory")
    parser.add_argument(
        "-dest-dir", type=Path, help="Path to the destination directory"
    )
    args = parser.parse_args()
    data_dir = args.data_dir
    dest_dir = args.dest_dir

    # rais-vinculos
    dataset = "vinculos"
    data_dir_rais_vinculos = data_dir / "rais-vinculos"
    dest_dir_rais_vinculos = dest_dir / "rais-vinculos"
    for data_raw_rais_year_dir in data_dir_rais_vinculos.iterdir():
        convert_rais_directory(data_raw_rais_year_dir, dataset, dest_dir_rais_vinculos)

    # rais-estabelecimentos
    dataset = "estabelecimentos"
    data_dir_rais_estabelecimentos = data_dir / "rais-estabelecimentos"
    dest_dir_rais_estabelecimentos = dest_dir / "rais-estabelecimentos"
    for data_raw_rais_year_dir in data_dir_rais_estabelecimentos.iterdir():
        convert_rais_directory(data_raw_rais_year_dir, dataset, dest_dir_rais_estabelecimentos)

    print("Done converting RAIS files.")


if __name__ == "__main__":
    main()

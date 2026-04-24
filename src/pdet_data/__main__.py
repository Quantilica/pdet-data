import argparse
import logging
from pathlib import Path

from . import (
    connect,
    fetch_caged,
    fetch_caged_2020,
    fetch_caged_2020_docs,
    fetch_caged_docs,
    fetch_rais,
    fetch_rais_docs,
    list_caged,
    list_caged_2020,
    list_caged_2020_docs,
    list_caged_docs,
    list_rais,
    list_rais_docs,
)

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")


def list_files_cmd(args):
    ftp = connect()
    try:
        for f in list_caged(ftp):
            dest_filepath = args.dest_dir / f["dataset"] / str(f["year"]) / f["name"]
            if dest_filepath.exists():
                continue
            print(f["full_path"], "-->", dest_filepath)
        for f in list_rais(ftp):
            dest_filepath = args.dest_dir / f["dataset"] / str(f["year"]) / f["name"]
            if dest_filepath.exists():
                continue
            print(f["full_path"], "-->", dest_filepath)
        for f in list_caged_2020(ftp):
            dest_filepath = args.dest_dir / f["dataset"] / str(f["year"]) / f["name"]
            if dest_filepath.exists():
                continue
            print(f["full_path"], "-->", dest_filepath)
    finally:
        ftp.close()


def fetch_cmd(args):
    ftp = connect()
    try:
        fetch_rais(ftp=ftp, dest_dir=args.dest_dir)
        fetch_rais_docs(ftp=ftp, dest_dir=args.dest_dir)
        fetch_caged(ftp=ftp, dest_dir=args.dest_dir)
        fetch_caged_docs(ftp=ftp, dest_dir=args.dest_dir)
        fetch_caged_2020(ftp=ftp, dest_dir=args.dest_dir)
        fetch_caged_2020_docs(ftp=ftp, dest_dir=args.dest_dir)
    finally:
        ftp.close()


def main():
    parser = argparse.ArgumentParser(
        description="Fetch and list Brazilian labor market microdata from PDET"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    fetch_parser = subparsers.add_parser("fetch", help="Fetch data from FTP server")
    fetch_parser.add_argument(
        "dest_dir",
        type=Path,
        help="Destination directory for downloaded files",
    )
    fetch_parser.set_defaults(func=fetch_cmd)

    list_parser = subparsers.add_parser("list", help="List available files on FTP server")
    list_parser.add_argument(
        "dest_dir",
        type=Path,
        help="Destination directory reference (to check what's already downloaded)",
    )
    list_parser.set_defaults(func=list_files_cmd)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return

    args.func(args)


if __name__ == "__main__":
    main()

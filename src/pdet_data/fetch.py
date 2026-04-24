import datetime as dt
import ftplib
import logging
import re
from pathlib import Path
from typing import Any, Callable, Generator, Sequence

from tqdm import tqdm

from .meta import datasets, docs
from .storage import (
    get_caged_2020_filepath,
    get_caged_filepath,
    get_docs_filepath,
    get_rais_filepath,
)

FTP_HOST = "ftp.mtps.gov.br"
logger = logging.getLogger(__name__)
_list_files_cache: dict[str, list[dict]] = {}


def connect() -> ftplib.FTP:
    ftp = ftplib.FTP(FTP_HOST, encoding="latin-1")
    ftp.login()
    return ftp


def list_files(ftp: ftplib.FTP, directory: str) -> list[dict]:
    """List all files in the current directory."""
    if directory in _list_files_cache:
        return _list_files_cache[directory]

    logger.info("Listing %s", directory)

    ftp_lines = []
    ftp.cwd(directory)
    ftp.retrlines("LIST", ftp_lines.append)

    # parse files' date, size and name
    def parse_line(line):
        m = re.match(
            r"^(\d{2}-\d{2}-\d{2}) +(\d{2}:\d{2})(AM|PM) +(<DIR>|\d+) +(.*)$",
            line,
        )
        if m:
            date, time, am_pm, size, name = m.groups()
            # parse datetime
            datetime = dt.datetime.strptime(
                f"{date} {time}{am_pm}",
                "%m-%d-%y %I:%M%p",
            )
            # parse size
            if size == "<DIR>":
                size = None
            else:
                size = int(size)
            # parse name
            name = name.strip()
            try:
                extension = name.rsplit(".", maxsplit=1)[1]
            except IndexError:
                extension = None
            file = {
                "datetime": datetime,
                "size": size,
                "name": name,
                "extension": extension,
                "full_path": f"{directory}/{name}",
            }
            return file
        else:
            return None

    files = []
    for f in ftp_lines:
        file = parse_line(f)
        if file:
            files.append(file)
    _list_files_cache[directory] = files
    return files


def fetch_file(ftp: ftplib.FTP, ftp_filepath: str, dest_filepath: Path, **kwargs):
    """Download a file from FTP."""
    dest_filepath.parent.mkdir(parents=True, exist_ok=True)

    if "file_size" in kwargs:
        file_size = kwargs["file_size"]
    else:
        file_size = ftp.size(ftp_filepath)

    logger.info("Fetching %s --> %s", ftp_filepath, dest_filepath)

    progress = tqdm(
        desc=dest_filepath.name,
        total=file_size,
        unit="B",
        unit_scale=True,
    )

    with open(dest_filepath, "wb") as f:

        def write(data):
            nonlocal f, progress
            f.write(data)
            progress.update(len(data))

        ftp.retrbinary(f"RETR {ftp_filepath}", write)
    progress.close()


def _get_date_dirs(
    fi: list[dict], dir_pattern: str, dir_pattern_groups: Sequence[str]
) -> list[dict]:
    """Filters list of directories in FTP server that groups files by date."""
    date_dirs = []
    for f in fi:
        if f["size"] is not None:
            continue
        m = re.match(dir_pattern, f["name"])
        if m:
            group_meta = {"dir": f["name"]}
            for i, group in enumerate(dir_pattern_groups):
                text = m.groups()[i]
                group_meta.update({group: text})
            date_dirs.append(group_meta)
    return date_dirs


def _get_group_meta(m: re.Match, variation: dict) -> dict:
    """Return a dictionary with info in a file name given by variation's
    fn_pattern.
    """
    group_meta = {}
    for group in variation["fn_pattern_groups"]:
        if not group:
            continue
        index = variation["fn_pattern_groups"].index(group)
        text = m.groups()[index].replace("_", "")
        group_meta.update({group: text})
    return group_meta


def _list_variation_files(
    ftp: ftplib.FTP, variation: dict
) -> Generator[dict, None, None]:
    ftp_path = variation["path"]
    if variation["dir_pattern"]:
        date_dirs = _get_date_dirs(
            fi=list_files(ftp, directory=ftp_path),
            dir_pattern=variation["dir_pattern"],
            dir_pattern_groups=variation["dir_pattern_groups"],
        )
        for date_dir_meta in date_dirs:
            date_dir = date_dir_meta["dir"]
            files = list_files(ftp, directory=f"{ftp_path}/{date_dir}")
            yield from (f | date_dir_meta for f in files)
    else:
        files = list_files(ftp, directory=ftp_path)
        yield from (f | {"year": None} for f in files)


def _get_variation_files_metadata(
    ftp: ftplib.FTP, variation: dict
) -> Generator[dict, None, None]:
    for file in _list_variation_files(ftp=ftp, variation=variation):
        m = re.match(
            variation["fn_pattern"],
            file["name"].lower(),
        )
        if m:
            group_meta = _get_group_meta(m, variation=variation)
            yield file | group_meta


def _list_dataset_files(ftp: ftplib.FTP, dataset: str) -> Generator[dict, None, None]:
    for variation in datasets[dataset]["variations"]:
        for f in _get_variation_files_metadata(ftp=ftp, variation=variation):
            yield f | {"dataset": dataset}


def _fetch_loop(
    ftp: ftplib.FTP,
    list_fn: Callable[[ftplib.FTP], Generator[dict, None, None]],
    get_filepath_fn: Callable[[dict, Path], Path],
    dest_dir: Path,
) -> list[dict[str, Any]]:
    metadata_list = []
    for file in list_fn(ftp):
        ftp_filepath = file["full_path"]
        dest_filepath = get_filepath_fn(file, dest_dir)
        if dest_filepath.exists() and dest_filepath.stat().st_size == file["size"]:
            logger.info("%s already fetched", ftp_filepath)
            continue
        file_size = file["size"]
        logger.info("Fetching %s --> %s", ftp_filepath, dest_filepath)
        fetch_file(ftp, ftp_filepath, dest_filepath, file_size=file_size)
        metadata = file | {"filepath": dest_filepath}
        metadata_list.append(metadata)
    return metadata_list


# -----------------------------------------------------------------------------
# ---------------------------------- CAGED ------------------------------------
# -----------------------------------------------------------------------------
def list_caged(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for dataset in ("caged", "caged-ajustes"):
        yield from _list_dataset_files(ftp=ftp, dataset=dataset)


def list_caged_docs(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for file in list_files(ftp, directory=docs["caged"]["dir_path"]):
        if not re.match(docs["caged"]["fn_pattern"], file["name"]):
            continue
        yield file | {"dataset": "caged"}
    for file in list_files(ftp, directory=docs["caged-ajustes"]["dir_path"]):
        if not re.match(docs["caged-ajustes"]["fn_pattern"], file["name"]):
            continue
        yield file | {"dataset": "caged-ajustes"}


def fetch_caged(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    return _fetch_loop(ftp, list_caged, get_caged_filepath, dest_dir)


def fetch_caged_docs(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    return _fetch_loop(ftp, list_caged_docs, get_docs_filepath, dest_dir)


def list_caged_2020(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for dataset in ("caged-2020-exc", "caged-2020-for", "caged-2020-mov"):
        yield from _list_dataset_files(ftp=ftp, dataset=dataset)


def list_caged_2020_docs(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for file in list_files(ftp, directory=docs["caged-2020"]["dir_path"]):
        if not re.match(docs["caged-2020"]["fn_pattern"], file["name"]):
            continue
        yield file | {"dataset": "caged-2020"}


def fetch_caged_2020(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    return _fetch_loop(ftp, list_caged_2020, get_caged_2020_filepath, dest_dir)


def fetch_caged_2020_docs(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    return _fetch_loop(ftp, list_caged_2020_docs, get_docs_filepath, dest_dir)


# -----------------------------------------------------------------------------
# ----------------------------------- RAIS ------------------------------------
# -----------------------------------------------------------------------------
def list_rais(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for dataset in ("rais-estabelecimentos", "rais-vinculos"):
        yield from _list_dataset_files(ftp=ftp, dataset=dataset)


def list_rais_docs(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for file in list_files(ftp, directory=docs["rais-vinculos"]["dir_path"]):
        yield file | {"dataset": "rais-vinculos"}
    for file in list_files(ftp, directory=docs["rais-estabelecimentos"]["dir_path"]):
        yield file | {"dataset": "rais-estabelecimentos"}


def fetch_rais(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    return _fetch_loop(ftp, list_rais, get_rais_filepath, dest_dir)


def fetch_rais_docs(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    return _fetch_loop(ftp, list_rais_docs, get_docs_filepath, dest_dir)

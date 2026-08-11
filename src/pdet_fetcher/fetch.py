import contextlib
import datetime as dt
import ftplib
import re
import time
from collections.abc import Generator, Sequence

from quantilica.core.ftp import FTP_TRANSIENT_ERRORS, FtpClient, ftp_connect
from quantilica.core.retry import exponential_delay

try:
    _RICH_AVAILABLE = True
except (ModuleNotFoundError, ImportError):  # pragma: no cover
    _RICH_AVAILABLE = False

from . import logger
from .meta import datasets, docs

FTP_HOST = "ftp.mtps.gov.br"
_PDET_FTP_TIMEOUT = 60.0

_list_files_cache: dict[str, list[dict]] = {}

# Global client for PDET
client = FtpClient(FTP_HOST)

# AttributeError handles the case where ftp.sock is None after a silent connection drop.
_FTP_ERRORS = FTP_TRANSIENT_ERRORS + (AttributeError,)


def connect(attempts: int = 3) -> ftplib.FTP:
    return ftp_connect(
        FTP_HOST,
        encoding="latin-1",
        timeout=_PDET_FTP_TIMEOUT,
        attempts=attempts,
        base_delay=2.0,
        max_delay=30.0,
        jitter=1.0,
    )


def list_files(directory: str) -> list[dict]:
    """List all files in the current directory using custom parser for MTPS server."""
    if directory in _list_files_cache:
        return _list_files_cache[directory]

    logger.info("Listing %s", directory)

    ftp_lines: list[str] = []
    last_exc: Exception | None = None
    for attempt in range(3):
        if attempt > 0:
            delay = exponential_delay(
                attempt, base_delay=2.0, max_delay=30.0, jitter=1.0
            )
            logger.warning(
                "FTP error on attempt %d listing %s: %s. Reconnecting in %.1fs...",
                attempt,
                directory,
                last_exc,
                delay,
            )
            time.sleep(delay)
        try:
            ftp = connect()
            try:
                ftp_lines = []
                ftp.cwd(directory)
                ftp.retrlines("LIST", ftp_lines.append)
            finally:
                with contextlib.suppress(Exception):
                    ftp.quit()
            break
        except _FTP_ERRORS as exc:
            last_exc = exc
            if attempt >= 2:
                raise

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
                "id": name,
                "url": f"{directory}/{name}".replace("//", "/"),
                "datetime": datetime,
                "size": size,
                "name": name,
                "extension": extension,
                "full_path": f"{directory}/{name}".replace("//", "/"),
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


def _get_date_dirs(
    fi: list[dict],
    dir_pattern: str | Sequence[str],
    dir_pattern_groups: Sequence[str] | Sequence[Sequence[str]],
) -> list[dict]:
    """Filters list of directories in FTP server that groups files by date."""
    if isinstance(dir_pattern, str):
        patterns = [dir_pattern]
        groups_list = [dir_pattern_groups]
    else:
        patterns = dir_pattern
        groups_list = dir_pattern_groups

    date_dirs = []
    for f in fi:
        if f["size"] is not None:
            continue
        for pattern, groups in zip(patterns, groups_list, strict=False):
            m = re.match(pattern, f["name"])
            if m:
                group_meta = {"dir": f["name"]}
                for i, group in enumerate(groups):
                    text = m.groups()[i]
                    group_meta.update({group: text})
                date_dirs.append(group_meta)
                break
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


def _list_variation_files(variation: dict) -> Generator[dict, None, None]:
    ftp_path = variation["path"]
    if variation["dir_pattern"]:
        date_dirs = _get_date_dirs(
            fi=list_files(ftp_path),
            dir_pattern=variation["dir_pattern"],
            dir_pattern_groups=variation["dir_pattern_groups"],
        )
        for date_dir_meta in date_dirs:
            date_dir = date_dir_meta["dir"]
            files = list_files(f"{ftp_path}/{date_dir}")
            yield from (f | date_dir_meta for f in files)
    else:
        files = list_files(ftp_path)
        yield from (f | {"year": None, "group": variation["group"]} for f in files)


def _get_variation_files_metadata(variation: dict) -> Generator[dict, None, None]:
    for file in _list_variation_files(variation=variation):
        m = re.match(
            variation["fn_pattern"],
            file["name"].lower(),
        )
        if m:
            group_meta = _get_group_meta(m, variation=variation)
            yield file | group_meta


def _list_dataset_files(dataset: str) -> Generator[dict, None, None]:
    for variation in datasets[dataset]["variations"]:
        variation = variation | {"group": dataset}
        for f in _get_variation_files_metadata(variation=variation):
            yield f | {"dataset": dataset, "group": dataset}


# -----------------------------------------------------------------------------
# ---------------------------------- CAGED ------------------------------------
# -----------------------------------------------------------------------------
def list_caged() -> Generator[dict, None, None]:
    for dataset in ("caged", "caged-ajustes"):
        yield from _list_dataset_files(dataset)


def list_caged_docs() -> Generator[dict, None, None]:
    for file in list_files(docs["caged"]["dir_path"]):
        if not re.match(docs["caged"]["fn_pattern"], file["name"]):
            continue
        yield file | {
            "dataset": "caged",
            "group": "caged",
            "year": None,
            "ext": file["extension"],
        }
    for file in list_files(docs["caged-ajustes"]["dir_path"]):
        if not re.match(docs["caged-ajustes"]["fn_pattern"], file["name"]):
            continue
        yield file | {
            "dataset": "caged-ajustes",
            "group": "caged-ajustes",
            "year": None,
            "ext": file["extension"],
        }


def list_caged_2020() -> Generator[dict, None, None]:
    for dataset in ("caged-2020-exc", "caged-2020-for", "caged-2020-mov"):
        yield from _list_dataset_files(dataset)


def list_caged_2020_docs() -> Generator[dict, None, None]:
    for file in list_files(docs["caged-2020"]["dir_path"]):
        if not re.match(docs["caged-2020"]["fn_pattern"], file["name"]):
            continue
        yield file | {
            "dataset": "caged-2020",
            "group": "caged-2020",
            "year": None,
            "ext": file["extension"],
        }


# -----------------------------------------------------------------------------
# ----------------------------------- RAIS ------------------------------------
# -----------------------------------------------------------------------------
def list_rais() -> Generator[dict, None, None]:
    for dataset in ("rais-estabelecimentos", "rais-vinculos"):
        yield from _list_dataset_files(dataset)


def list_rais_docs() -> Generator[dict, None, None]:
    for file in list_files(docs["rais-vinculos"]["dir_path"]):
        yield file | {
            "dataset": "rais-vinculos",
            "group": "rais-vinculos",
            "year": None,
            "ext": file["extension"],
        }
    for file in list_files(docs["rais-estabelecimentos"]["dir_path"]):
        yield file | {
            "dataset": "rais-estabelecimentos",
            "group": "rais-estabelecimentos",
            "year": None,
            "ext": file["extension"],
        }

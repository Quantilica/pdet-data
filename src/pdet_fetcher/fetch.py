import contextlib
import datetime as dt
import ftplib
import re
import time
from collections.abc import Callable, Generator, Sequence
from pathlib import Path
from typing import Any

import quantilica.core.metadata as core_meta
from quantilica.core.ftp import FTP_TRANSIENT_ERRORS, FtpClient, ftp_connect
from quantilica.core.retry import exponential_delay
from tqdm import tqdm as _tqdm

try:
    from quantilica.core.cli import (
        get_console,
        make_batch_progress,
        make_download_progress,
    )
    from rich.console import Group
    from rich.live import Live

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
        yield from (f | {"year": None} for f in files)


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
        for f in _get_variation_files_metadata(variation=variation):
            yield f | {"dataset": dataset}


def _fetch_loop(
    list_fn: Callable[[], Generator[dict, None, None]],
    get_filepath_fn: Callable[[dict, Path], Path],
    dest_dir: Path,
    show_progress: bool = False,
    workers: int = 4,
) -> list[dict[str, Any]]:
    import concurrent.futures

    metadata_list = []
    files = list(list_fn())
    if not files:
        return metadata_list

    groups: dict[str, list[dict]] = {}
    for f in files:
        groups.setdefault(f.get("dataset", "unknown"), []).append(f)

    live = None
    batch_progress = file_progress = None

    if show_progress and _RICH_AVAILABLE:
        console = get_console()
        batch_progress = make_batch_progress(console)
        file_progress = make_download_progress(console)
        live = Live(
            Group(batch_progress, file_progress),
            console=console,
            refresh_per_second=10,
        )
        live.start()

        import threading

        lock = threading.Lock()
        worker_task_ids = [
            file_progress.add_task("[dim]Inativo[/dim]", total=1)
            for _ in range(workers)
        ]
        available_tasks = worker_task_ids.copy()

    def _download_file(file, batch_task, pbar):
        ftp_filepath = file["full_path"]
        dest_filepath = get_filepath_fn(file, dest_dir)

        task_id = None
        if file_progress is not None:
            with lock:
                task_id = available_tasks.pop(0)
            file_progress.update(
                task_id,
                description=f"[cyan]{file['name']}[/cyan]",
                completed=0,
                total=file["size"] or None,
            )

        def _chunk_cb(n: int, _tid=task_id) -> None:
            if _tid is not None:
                file_progress.update(_tid, advance=n)

        try:
            downloaded_path = client.download_with_manifest(
                ftp_filepath,
                dest_filepath,
                source_id="pdet",
                dataset_id=file.get("dataset", "unknown"),
                producer="pdet-fetcher",
                progress_callback=_chunk_cb if task_id is not None else None,
            )
            return file | {"filepath": downloaded_path}
        except Exception as e:
            logger.error(f"Failed to download {ftp_filepath}: {e}")
            return None
        finally:
            if task_id is not None:
                with lock:
                    file_progress.update(
                        task_id, description="[dim]Inativo[/dim]", completed=0, total=1
                    )
                    available_tasks.append(task_id)
            if batch_task is not None:
                batch_progress.update(batch_task, advance=1)
            elif pbar is not None:
                pbar.update(file["size"] or 0)

    try:
        for dataset_name, dataset_files in groups.items():
            batch_task = None
            pbar = None

            if show_progress:
                if batch_progress is not None:
                    batch_task = batch_progress.add_task(
                        f"[cyan]{dataset_name}[/cyan]",
                        total=len(dataset_files),
                    )
                else:
                    total_bytes = sum(f["size"] or 0 for f in dataset_files)
                    pbar = _tqdm(
                        total=total_bytes,
                        desc=dataset_name,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        leave=True,
                    )

            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [
                    executor.submit(_download_file, f, batch_task, pbar)
                    for f in dataset_files
                ]
                for future in concurrent.futures.as_completed(futures):
                    res = future.result()
                    if res:
                        metadata_list.append(res)

            if pbar is not None:
                pbar.close()
    finally:
        if live is not None:
            with contextlib.suppress(Exception):
                live.stop()

    return metadata_list


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
        yield file | {"dataset": "caged"}
    for file in list_files(docs["caged-ajustes"]["dir_path"]):
        if not re.match(docs["caged-ajustes"]["fn_pattern"], file["name"]):
            continue
        yield file | {"dataset": "caged-ajustes"}


def fetch_caged(
    dest_dir: Path, show_progress: bool = False, workers: int = 4
) -> list[dict[str, Any]]:
    from .storage import get_caged_filepath

    return _fetch_loop(list_caged, get_caged_filepath, dest_dir, show_progress, workers)


def fetch_caged_docs(
    dest_dir: Path, show_progress: bool = False, workers: int = 4
) -> list[dict[str, Any]]:
    from .storage import get_docs_filepath

    return _fetch_loop(
        list_caged_docs, get_docs_filepath, dest_dir, show_progress, workers
    )


def list_caged_2020() -> Generator[dict, None, None]:
    for dataset in ("caged-2020-exc", "caged-2020-for", "caged-2020-mov"):
        yield from _list_dataset_files(dataset)


def list_caged_2020_docs() -> Generator[dict, None, None]:
    for file in list_files(docs["caged-2020"]["dir_path"]):
        if not re.match(docs["caged-2020"]["fn_pattern"], file["name"]):
            continue
        yield file | {"dataset": "caged-2020"}


def fetch_caged_2020(
    dest_dir: Path, show_progress: bool = False, workers: int = 4
) -> list[dict[str, Any]]:
    from .storage import get_caged_2020_filepath

    return _fetch_loop(
        list_caged_2020, get_caged_2020_filepath, dest_dir, show_progress, workers
    )


def fetch_caged_2020_docs(
    dest_dir: Path, show_progress: bool = False, workers: int = 4
) -> list[dict[str, Any]]:
    from .storage import get_docs_filepath

    return _fetch_loop(
        list_caged_2020_docs, get_docs_filepath, dest_dir, show_progress, workers
    )


# -----------------------------------------------------------------------------
# ----------------------------------- RAIS ------------------------------------
# -----------------------------------------------------------------------------
def list_rais() -> Generator[dict, None, None]:
    for dataset in ("rais-estabelecimentos", "rais-vinculos"):
        yield from _list_dataset_files(dataset)


def list_rais_docs() -> Generator[dict, None, None]:
    for file in list_files(docs["rais-vinculos"]["dir_path"]):
        yield file | {"dataset": "rais-vinculos"}
    for file in list_files(docs["rais-estabelecimentos"]["dir_path"]):
        yield file | {"dataset": "rais-estabelecimentos"}


def fetch_rais(
    dest_dir: Path, show_progress: bool = False, workers: int = 4
) -> list[dict[str, Any]]:
    from .storage import get_rais_filepath

    return _fetch_loop(list_rais, get_rais_filepath, dest_dir, show_progress, workers)


def fetch_rais_docs(
    dest_dir: Path, show_progress: bool = False, workers: int = 4
) -> list[dict[str, Any]]:
    from .storage import get_docs_filepath

    return _fetch_loop(
        list_rais_docs, get_docs_filepath, dest_dir, show_progress, workers
    )


def generate_catalog(
    downloaded_files: list[dict],
) -> core_meta.MetadataCatalog:
    """Generate a validated MetadataCatalog from a list of downloaded PDET files."""
    source_id = "pdet"
    source = core_meta.Source(
        id=source_id,
        name="PDET - Programa de Disseminação de Estatísticas do Trabalho",
        homepage_url="http://pdet.mte.gov.br",
    )

    datasets_map = {}
    resources = []

    for file in downloaded_files:
        dataset_id = file.get("dataset", "unknown")
        if dataset_id not in datasets_map:
            datasets_map[dataset_id] = core_meta.Dataset(
                id=dataset_id,
                source_id=source_id,
                name=dataset_id.upper().replace("-", " "),
            )

        filename = file["filepath"].name
        resource_id = filename.replace(".", "_")

        resources.append(
            core_meta.Resource(
                id=resource_id,
                dataset_id=dataset_id,
                name=filename,
                url=file["full_path"],  # FTP path
                format=file.get("extension", ""),
                path=str(file["filepath"].absolute()),
                metadata={
                    "remote_datetime": file["datetime"].isoformat(),
                    "size": file["size"],
                },
            )
        )

    catalog = core_meta.MetadataCatalog(
        sources=[source],
        datasets=list(datasets_map.values()),
        resources=resources,
    )
    catalog.validate_references()
    return catalog

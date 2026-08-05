"""CLI standalone para pdet-fetcher."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from quantilica.core.logging import configure_cli_logging

from . import (
    __version__,
    connect,
    convert_caged,
    convert_rais,
    extract_columns_for_dataset,
    fetch_caged,
    fetch_caged_2020,
    fetch_caged_2020_docs,
    fetch_caged_docs,
    fetch_rais,
    fetch_rais_docs,
    list_caged,
    list_caged_2020,
    list_rais,
)

_DEFAULT_OUTPUT = Path("/data/pdet")

_DATASET_FETCHERS = {
    "rais": (fetch_rais, fetch_rais_docs),
    "caged": (fetch_caged, fetch_caged_docs),
    "caged-2020": (fetch_caged_2020, fetch_caged_2020_docs),
}

_DATASETS = {
    "rais-estabelecimentos": {
        "glob_pattern": "rais-*.*",
        "has_uf": True,
        "encoding": "latin-1",
    },
    "rais-vinculos": {
        "glob_pattern": "rais-*.*",
        "has_uf": True,
        "encoding": "latin-1",
    },
    "caged": {
        "glob_pattern": "caged_*.*",
        "has_uf": False,
        "encoding": "latin-1",
    },
    "caged-ajustes": {
        "glob_pattern": "caged-ajustes_*.*",
        "has_uf": False,
        "encoding": "latin-1",
    },
    "caged-2020": {
        "glob_pattern": "caged-2020-*.*",
        "has_uf": False,
        "encoding": "utf-8",
    },
}


def _resolve_targets(datasets: list[str] | None) -> list[str]:
    targets = datasets if datasets else list(_DATASET_FETCHERS.keys())
    invalid = [d for d in targets if d not in _DATASET_FETCHERS]
    if invalid:
        raise SystemExit(f"Erro: dataset(s) desconhecido(s): {', '.join(invalid)}")
    return targets


def _run_sync(
    targets: list[str], output: Path, show_progress: bool, workers: int = 4
) -> None:
    for dataset in targets:
        data_fn, docs_fn = _DATASET_FETCHERS[dataset]
        data_fn(dest_dir=output, show_progress=show_progress, workers=workers)
        docs_fn(dest_dir=output, show_progress=show_progress, workers=workers)


def handle_sync(args: argparse.Namespace) -> None:
    targets = _resolve_targets(args.datasets)
    _run_sync(
        targets, args.output, show_progress=not args.verbose, workers=args.workers
    )


def handle_list(args: argparse.Namespace) -> None:
    ftp = connect()
    try:
        for listing in (list_caged, list_caged_2020, list_rais):
            for f in listing(ftp):
                dest = args.output / f["dataset"] / str(f["year"]) / f["name"]
                if not dest.exists():
                    print(f["full_path"], "-->", dest)
    finally:
        ftp.close()


def handle_convert(args: argparse.Namespace) -> None:
    convert_rais(args.input, args.output)
    convert_caged(args.input, args.output)


def handle_columns(args: argparse.Namespace) -> None:
    if args.dataset not in _DATASETS:
        raise SystemExit(f"Erro: dataset desconhecido: {args.dataset}")
    cfg = _DATASETS[args.dataset]
    output_file = args.output / f"{args.dataset}-columns.csv"
    extract_columns_for_dataset(
        args.input,
        cfg["glob_pattern"],
        output_file,
        encoding=cfg["encoding"],
        has_uf=cfg["has_uf"],
    )


def handle_pipeline(args: argparse.Namespace) -> None:
    targets = _resolve_targets(args.datasets)
    parquet_out = args.parquet_dir or args.output
    _run_sync(
        targets, args.output, show_progress=not args.verbose, workers=args.workers
    )
    convert_rais(args.output, parquet_out)
    convert_caged(args.output, parquet_out)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pdet-fetcher",
        description="Microdados do PDET (CAGED, RAIS).",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Exibir logs detalhados em vez de barra de progresso",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # sync
    p_sync = subparsers.add_parser(
        "sync", help="Sincronizar microdados do PDET via FTP"
    )
    p_sync.add_argument(
        "datasets",
        nargs="*",
        help="Datasets (rais, caged, caged-2020). Omitir para todos.",
    )
    p_sync.add_argument(
        "-o",
        "--output",
        type=Path,
        default=_DEFAULT_OUTPUT,
        help="Diretório de destino (padrão: /data/pdet)",
    )
    p_sync.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Número de downloads paralelos",
    )
    p_sync.set_defaults(func=handle_sync)

    # list
    p_list = subparsers.add_parser("list", help="Listar arquivos disponíveis no FTP")
    p_list.add_argument(
        "-o",
        "--output",
        type=Path,
        default=_DEFAULT_OUTPUT,
        help="Diretório de referência (padrão: /data/pdet)",
    )
    p_list.set_defaults(func=handle_list)

    # convert
    p_convert = subparsers.add_parser(
        "convert", help="Converter arquivos brutos para Parquet"
    )
    p_convert.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Diretório de origem com arquivos brutos",
    )
    p_convert.add_argument(
        "-o",
        "--output",
        type=Path,
        default=_DEFAULT_OUTPUT,
        help="Diretório de destino para Parquet (padrão: /data/pdet)",
    )
    p_convert.set_defaults(func=handle_convert)

    # columns
    p_columns = subparsers.add_parser(
        "columns", help="Extrair nomes de colunas dos arquivos brutos"
    )
    p_columns.add_argument(
        "dataset",
        help=f"Dataset ({', '.join(_DATASETS.keys())})",
    )
    p_columns.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Diretório de origem com arquivos brutos",
    )
    p_columns.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path.cwd(),
        help="Diretório para CSV de colunas (padrão: diretório atual)",
    )
    p_columns.set_defaults(func=handle_columns)

    # pipeline
    p_pipeline = subparsers.add_parser(
        "pipeline", help="Pipeline completo (sync -> convert)"
    )
    p_pipeline.add_argument(
        "datasets",
        nargs="*",
        help="Datasets (rais, caged, caged-2020). Omitir para todos.",
    )
    p_pipeline.add_argument(
        "-o",
        "--output",
        type=Path,
        default=_DEFAULT_OUTPUT,
        help="Diretório de dados brutos (padrão: /data/pdet)",
    )
    p_pipeline.add_argument(
        "--parquet-dir",
        type=Path,
        default=None,
        help="Diretório para os Parquet (padrão: igual a --output)",
    )
    p_pipeline.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Número de downloads paralelos",
    )
    p_pipeline.set_defaults(func=handle_pipeline)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = get_parser()
    args = parser.parse_args(argv)
    configure_cli_logging(verbose=args.verbose)
    if not args.verbose:
        logging.getLogger("quantilica.core").setLevel(logging.WARNING)
        logging.getLogger("pdet_fetcher").setLevel(logging.WARNING)
    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\nOperação cancelada pelo usuário.", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()

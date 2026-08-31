# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Typer plugin for quantilica-cli integration."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Annotated, Any

import typer
from quantilica.cli.sdk import FetcherApp
from quantilica.cli.ui import get_console, setup_rich_logging
from quantilica.core.ftp import FtpClient
from rich.rule import Rule

from pdet_fetcher import (
    list_caged,
    list_caged_2020,
    list_caged_2020_docs,
    list_caged_docs,
    list_rais,
    list_rais_docs,
)
from pdet_fetcher.storage import (
    get_caged_2020_docs_filepath,
    get_caged_2020_filepath,
    get_caged_docs_filepath,
    get_caged_filepath,
    get_rais_docs_filepath,
    get_rais_filepath,
)

from .fetch import FTP_HOST

_DEFAULT_OUTPUT = Path("/data/pdet")
console = get_console()

GROUPS = {
    "caged": {"name": "CAGED Histórico (até 2019)"},
    "caged-2020": {"name": "Novo CAGED (a partir de 2020)"},
    "rais": {"name": "RAIS (Vínculos e Estabelecimentos)"},
}


def pdet_list_datasets(group: str) -> list[dict[str, Any]]:
    """Lists datasets for a given group.

    Args:
        group (str): The name of the dataset group.

    Returns:
        list[dict[str, Any]]: A list of dictionaries with dataset information.
    """
    if group == "caged":
        return list(list_caged()) + list(list_caged_docs())
    elif group == "caged-2020":
        return list(list_caged_2020()) + list(list_caged_2020_docs())
    elif group == "rais":
        return list(list_rais()) + list(list_rais_docs())
    return []


def pdet_path_builder(
    output_dir: Path, entry: dict[str, Any], last_modified: dt.date | None
) -> Path:
    """Builds the file path for a dataset entry.

    Args:
        output_dir (Path): The base output directory.
        entry (dict[str, Any]): The dataset entry metadata.
        last_modified (dt.date | None): The last modified date of the entry.

    Returns:
        Path: The constructed file path.
    """
    # A base is the target dir
    dataset = entry.get("dataset")

    # We figure out if it's docs or data based on the path.
    # Actually, we can use the original storage functions.
    is_doc = "/_documentacao/" in entry.get("url", "")

    if dataset == "caged" or dataset == "caged-ajustes":
        if is_doc:
            return get_caged_docs_filepath(entry, output_dir)
        return get_caged_filepath(entry, output_dir)
    elif dataset == "caged-2020":
        if is_doc:
            return get_caged_2020_docs_filepath(entry, output_dir)
        return get_caged_2020_filepath(entry, output_dir)
    elif dataset in ("rais-estabelecimentos", "rais-vinculos"):
        if is_doc:
            return get_rais_docs_filepath(entry, output_dir)
        return get_rais_filepath(entry, output_dir)

    # fallback
    return output_dir / (entry.get("id") or "unknown.bin")


fetcher = FetcherApp(
    name="pdet-fetcher",
    help="Microdados do PDET (CAGED, RAIS).",
    groups_dict=GROUPS,
    aliases_dict={},
    list_datasets=pdet_list_datasets,
    path_builder=pdet_path_builder,
    default_output=_DEFAULT_OUTPUT,
    client=FtpClient(FTP_HOST),
)

app = fetcher.app

# Datasets reconhecidos pelo extrator de colunas.
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


@app.command("convert")
def cmd_convert(
    input: Annotated[
        Path,
        typer.Option("-i", "--input", help="Diretório de origem com arquivos brutos"),
    ],
    output: Annotated[
        Path,
        typer.Option("-o", "--output", help="Diretório de destino para Parquet"),
    ] = _DEFAULT_OUTPUT,
    verbose: Annotated[bool, typer.Option("--verbose", help="Logs detalhados")] = False,
) -> None:
    """Converter arquivos brutos para Parquet.

    Args:
        input (Path): Diretório de origem com arquivos brutos.
        output (Path, optional): Diretório de destino para Parquet.
            Defaults to _DEFAULT_OUTPUT.
        verbose (bool, optional): Exibir logs detalhados. Defaults to False.

    Raises:
        typer.Exit: If required dependencies are missing.
    """
    setup_rich_logging(verbose, console=console)
    try:
        from pdet_fetcher import convert_caged, convert_rais

        if convert_rais is None or convert_caged is None:
            raise ImportError
    except ImportError:
        console.print(
            "[red]Erro:[/red] convert requer extras de análise: "
            "pip install pdet-fetcher[analysis]"
        )
        raise typer.Exit(1) from None

    convert_rais(input, output)
    convert_caged(input, output)
    console.print("[green]✓[/green] Conversão concluída.")


@app.command("columns")
def cmd_columns(
    dataset: Annotated[
        str,
        typer.Argument(help=f"Dataset ({', '.join(_DATASETS.keys())})"),
    ],
    input: Annotated[
        Path,
        typer.Option("-i", "--input", help="Diretório de origem com arquivos brutos"),
    ],
    output: Annotated[
        Path,
        typer.Option("-o", "--output", help="Diretório para CSV de colunas"),
    ] = Path("."),
    verbose: Annotated[bool, typer.Option("--verbose", help="Logs detalhados")] = False,
) -> None:
    """Extrair nomes de colunas dos arquivos brutos.

    Args:
        dataset (str): The name of the dataset.
        input (Path): Diretório de origem com arquivos brutos.
        output (Path, optional): Diretório para CSV de colunas. Defaults to
            current directory.
        verbose (bool, optional): Exibir logs detalhados. Defaults to False.

    Raises:
        typer.Exit: If the dataset is unknown or required dependencies are missing.
    """
    setup_rich_logging(verbose, console=console)
    if dataset not in _DATASETS:
        console.print(f"[red]Dataset desconhecido:[/red] {dataset}")
        raise typer.Exit(1)

    try:
        from pdet_fetcher import extract_columns_for_dataset

        if extract_columns_for_dataset is None:
            raise ImportError
    except ImportError:
        console.print(
            "[red]Erro:[/red] columns requer extras de análise: "
            "pip install pdet-fetcher[analysis]"
        )
        raise typer.Exit(1) from None

    cfg = _DATASETS[dataset]
    output_file = output / f"{dataset}-columns.csv"
    extract_columns_for_dataset(
        input,
        cfg["glob_pattern"],
        output_file,
        encoding=cfg["encoding"],
        has_uf=cfg["has_uf"],
    )
    console.print(f"[green]✓[/green] Colunas salvas em [bold]{output_file}[/bold]")


@app.command("pipeline")
def cmd_pipeline(
    datasets: Annotated[
        list[str] | None,
        typer.Argument(
            help="Datasets (rais, caged, caged-2020). Omitir para todos.",
        ),
    ] = None,
    output: Annotated[
        Path,
        typer.Option("-o", "--output", help="Diretório de dados brutos"),
    ] = _DEFAULT_OUTPUT,
    parquet_dir: Annotated[
        Path | None,
        typer.Option(
            "--parquet-dir",
            help="Diretório para os Parquet (padrão: igual a --output)",
        ),
    ] = None,
    workers: Annotated[
        int, typer.Option("--workers", help="Número de downloads paralelos")
    ] = 4,
    verbose: Annotated[bool, typer.Option("--verbose", help="Logs detalhados")] = False,
) -> None:
    """Pipeline completo do PDET (sync → convert).

    Args:
        datasets (list[str] | None, optional): Datasets to process.
            Defaults to None.
        output (Path, optional): Diretório de dados brutos. Defaults to
            _DEFAULT_OUTPUT.
        parquet_dir (Path | None, optional): Diretório para os Parquet.
            Defaults to None.
        workers (int, optional): Número de downloads paralelos. Defaults
            to 4.
        verbose (bool, optional): Exibir logs detalhados. Defaults to False.

    Raises:
        typer.Exit: If a dataset is unknown, dependencies are missing, or user cancels.
    """
    setup_rich_logging(verbose, console=console)
    targets = datasets if datasets else list(GROUPS.keys())
    invalid = [d for d in targets if d not in GROUPS]
    if invalid:
        console.print(
            f"[red]Erro:[/red] dataset(s) desconhecido(s): {', '.join(invalid)}"
        )
        raise typer.Exit(1)
    parquet_out = parquet_dir or output

    try:
        console.print(Rule("[bold]Passo 1/2: Download[/bold]"))
        entries = [e for g in targets for e in fetcher.list_datasets(g)]

        fetcher.download_datasets(entries, output, workers=workers)

        console.print("[green]✓[/green] Download concluído.")

        console.print(Rule("[bold]Passo 2/2: Conversão[/bold]"))
        try:
            from pdet_fetcher import convert_caged, convert_rais

            if convert_caged is None or convert_rais is None:
                raise ImportError
        except ImportError:
            console.print(
                "[red]Erro:[/red] pipeline (conversão) requer extras de análise: "
                "pip install pdet-fetcher[analysis]"
            )
            raise typer.Exit(1) from None

        convert_rais(output, parquet_out)
        convert_caged(output, parquet_out)
        console.print(f"[green]✓[/green] Parquet salvo em [dim]{parquet_out}[/dim]")
    except KeyboardInterrupt as err:
        console.print("[yellow]Pipeline cancelado pelo usuário.[/yellow]")
        raise typer.Exit(code=130) from err

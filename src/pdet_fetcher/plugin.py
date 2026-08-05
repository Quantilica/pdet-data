# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Typer plugin for quantilica-cli integration."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from quantilica.core.cli import get_console, setup_rich_logging
from rich.rule import Rule
from rich.table import Table

from pdet_fetcher import (
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

app = typer.Typer(help="Microdados do PDET (CAGED, RAIS).")

_DEFAULT_OUTPUT = Path("/data/pdet")
console = get_console()

# Datasets baixáveis e os fetchers (dados + docs) de cada um.
_DATASET_FETCHERS = {
    "rais": (fetch_rais, fetch_rais_docs),
    "caged": (fetch_caged, fetch_caged_docs),
    "caged-2020": (fetch_caged_2020, fetch_caged_2020_docs),
}

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


def _run_sync(
    targets: list[str], output: Path, show_progress: bool, workers: int = 4
) -> None:
    """Baixar os datasets selecionados via FTP."""
    for dataset in targets:
        data_fn, docs_fn = _DATASET_FETCHERS[dataset]
        data_fn(dest_dir=output, show_progress=show_progress, workers=workers)
        docs_fn(dest_dir=output, show_progress=show_progress, workers=workers)


@app.command("sync")
def cmd_sync(
    datasets: Annotated[
        list[str] | None,
        typer.Argument(
            help=("Datasets (rais, caged, caged-2020). Omitir para todos."),
        ),
    ] = None,
    output: Annotated[
        Path, typer.Option("-o", "--output", help="Diretório de destino")
    ] = _DEFAULT_OUTPUT,
    workers: Annotated[
        int, typer.Option("--workers", help="Número de downloads paralelos")
    ] = 4,
    verbose: Annotated[bool, typer.Option("--verbose", help="Logs detalhados")] = False,
) -> None:
    """Sincronizar microdados do PDET via FTP."""
    setup_rich_logging(verbose, console=console)
    targets = datasets if datasets else list(_DATASET_FETCHERS.keys())
    invalid = [d for d in targets if d not in _DATASET_FETCHERS]
    if invalid:
        console.print(
            f"[red]Erro:[/red] dataset(s) desconhecido(s): {', '.join(invalid)}"
        )
        raise typer.Exit(1)

    try:
        _run_sync(targets, output, show_progress=not verbose, workers=workers)
        console.print("[green]✓[/green] Sincronização concluída.")
    except KeyboardInterrupt as err:
        console.print("[yellow]Sincronização cancelada pelo usuário.[/yellow]")
        raise typer.Exit(code=130) from err


@app.command("list")
def cmd_list(
    output: Annotated[
        Path, typer.Option("-o", "--output", help="Diretório de referência")
    ] = _DEFAULT_OUTPUT,
    verbose: Annotated[bool, typer.Option("--verbose", help="Logs detalhados")] = False,
) -> None:
    """Listar arquivos disponíveis no FTP."""
    setup_rich_logging(verbose, console=console)
    t = Table(show_header=True, header_style="bold")
    t.add_column("Dataset", style="cyan")
    t.add_column("Ano", justify="right")
    t.add_column("Arquivo")
    t.add_column("Destino")

    for listing in (list_caged, list_caged_2020, list_rais):
        for f in listing():
            dest = output / f["dataset"] / str(f["year"]) / f["name"]
            if not dest.exists():
                t.add_row(f["dataset"], str(f["year"]), f["name"], str(dest))
    console.print(t)


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
    """Converter arquivos brutos para Parquet."""
    setup_rich_logging(verbose, console=console)
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
    """Extrair nomes de colunas dos arquivos brutos."""
    setup_rich_logging(verbose, console=console)
    if dataset not in _DATASETS:
        console.print(f"[red]Dataset desconhecido:[/red] {dataset}")
        raise typer.Exit(1)
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
    """Pipeline completo do PDET (sync → convert)."""
    setup_rich_logging(verbose, console=console)
    targets = datasets if datasets else list(_DATASET_FETCHERS.keys())
    invalid = [d for d in targets if d not in _DATASET_FETCHERS]
    if invalid:
        console.print(
            f"[red]Erro:[/red] dataset(s) desconhecido(s): {', '.join(invalid)}"
        )
        raise typer.Exit(1)
    parquet_out = parquet_dir or output

    try:
        console.print(Rule("[bold]Passo 1/2: Download[/bold]"))
        _run_sync(targets, output, show_progress=not verbose, workers=workers)
        console.print("[green]✓[/green] Download concluído.")

        console.print(Rule("[bold]Passo 2/2: Conversão[/bold]"))
        convert_rais(output, parquet_out)
        convert_caged(output, parquet_out)
        console.print(f"[green]✓[/green] Parquet salvo em [dim]{parquet_out}[/dim]")
    except KeyboardInterrupt as err:
        console.print("[yellow]Pipeline cancelado pelo usuário.[/yellow]")
        raise typer.Exit(code=130) from err

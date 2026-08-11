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
    """Converter arquivos brutos para Parquet."""
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
    """Extrair nomes de colunas dos arquivos brutos."""
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
    """Pipeline completo do PDET (sync → convert)."""
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
        import concurrent.futures

        from quantilica.cli.ui import (
            ProgressPool,
            graceful_executor,
            make_batch_progress,
            make_download_progress,
        )
        from rich.console import Group
        from rich.live import Live

        entries = [e for g in targets for e in fetcher.list_datasets(g)]

        overall = make_batch_progress(console)
        file_prog = make_download_progress(console)
        overall_task = overall.add_task("[cyan]Baixando...[/cyan]", total=len(entries))
        pool = ProgressPool(workers=workers, file_prog=file_prog)

        def _worker(entry):
            eid = entry.get("id", "unknown")
            with pool.acquire(description=f"[cyan]{eid}[/cyan]") as cb:
                fetcher.download_entry(entry, output, progress=cb)
                return True

        with graceful_executor(max_workers=workers) as executor:
            with Live(
                Group(overall, file_prog), console=console, refresh_per_second=10
            ):
                futures = {executor.submit(_worker, entry): entry for entry in entries}
                for future in concurrent.futures.as_completed(futures):
                    overall.update(overall_task, advance=1)
                    future.result()

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

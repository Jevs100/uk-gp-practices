"""Command line interface for downloading and querying UK GP practice data."""

from __future__ import annotations

import json
import typer

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from .index import PracticeIndex
from .paths import csv_path, db_path as default_db_path
from .sources import ALL_SOURCES


app = typer.Typer(help="Query UK GP practices (surgeries) from cached public NHS data.")
console = Console()


@app.command()
def update() -> None:
    """
    Download all supported sources and refresh the local SQLite database.
    """
    idx = PracticeIndex(db_file=default_db_path())

    for source in ALL_SOURCES:
        csvf = csv_path(source.nation)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(f"Downloading {source.nation}...", total=None)

            def on_progress(completed: int, total: int | None, _t=task) -> None:
                progress.update(_t, completed=completed, total=total)

            result = source.download(dest=csvf, on_progress=on_progress)
            progress.update(task, completed=result.bytes_written, total=result.bytes_written)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(f"Loading {source.nation} into database...", total=None)

            def on_db_progress(completed: int, total: int, _t=task) -> None:
                progress.update(_t, completed=completed, total=total)

            idx.load_source(csvf, source=source, on_progress=on_db_progress)

    console.print(f"[green]✓[/green] Done — DB ready at: {idx.db_file}")


@app.command()
def get(code: str) -> None:
    """
    Print a single practice record by organisation code.
    """
    idx = PracticeIndex.auto_update()
    p = idx.get(code)
    if not p:
        typer.echo(f"Practice not found: {code}", err=True)
        raise typer.Exit(code=1)
    typer.echo(json.dumps(p.raw or {}, indent=2))


@app.command()
def search(
    name: str = typer.Option("", help="Search by practice name (substring match)."),
    postcode: str = typer.Option(
        "", help="Search by postcode (exact after normalization)."
    ),
    town: str = typer.Option("", help="Search by town (substring match)."),
    status: str = typer.Option(
        "", help="Filter by status (e.g. ACTIVE). Leave blank for any."
    ),
    nation: str = typer.Option(
        "", help="Filter by nation (e.g. england, scotland, northern_ireland)."
    ),
    limit: int = typer.Option(10, help="Max results."),
) -> None:
    """
    Print practice records matching the supplied filters.
    """
    idx = PracticeIndex.auto_update()
    res = idx.search(
        name=name or None,
        postcode=postcode or None,
        town=town or None,
        status=status or None,
        nation=nation or None,
        limit=limit,
    )
    typer.echo(json.dumps([r.raw for r in res], indent=2))

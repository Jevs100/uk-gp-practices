"""Typer CLI for querying UK GP practices (surgeries) via NHS ODS DSE CSV reports."""

from __future__ import annotations

import json
import typer

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from .download import download_report
from .index import PracticeIndex
from .paths import csv_path, db_path as default_db_path


app = typer.Typer(help="Query UK GP practices (surgeries) via NHS ODS DSE CSV reports.")
console = Console()


@app.command()
def update(report: str = "epraccur") -> None:
    """
    Download the latest report and update the local database.
    """
    idx = PracticeIndex(db_file=default_db_path())
    idx._ensure_schema()
    csvf = csv_path(report)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"Downloading {report}...", total=None)

        def on_progress(completed: int, total: int | None) -> None:
            progress.update(task, completed=completed, total=total)

        result = download_report(report=report, dest=csvf, on_progress=on_progress)
        progress.update(task, completed=result.bytes_written, total=result.bytes_written)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Loading into database...", total=None)
        idx.load_csv(csvf, report=report)

    console.print(f"[green]✓[/green] Done — DB ready at: {idx.db_file}")


@app.command()
def get(code: str, report: str = "epraccur") -> None:
    """
    Get a single practice by organisation code (ODS code).
    """
    idx = PracticeIndex.auto_update(report=report)
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
    limit: int = typer.Option(10, help="Max results."),
    report: str = typer.Option("epraccur", help="ODS report code."),
) -> None:
    """
    Search practices by name/postcode/town.
    """
    idx = PracticeIndex.auto_update(report=report)
    res = idx.search(
        name=name or None,
        postcode=postcode or None,
        town=town or None,
        status=status or None,
        limit=limit,
    )
    typer.echo(json.dumps([r.raw for r in res], indent=2))

"""
Entrypoint du pipeline recipes.

Usage :
    python run_pipeline.py run              # pipeline complet (dlt + SQL)
    python run_pipeline.py run --dest duckdb
    python run_pipeline.py run --dest delta  # filesystem Delta Lake
    python run_pipeline.py ingest            # dlt uniquement (staging)
    python run_pipeline.py transform         # SQL uniquement (sur staging existant)
    python run_pipeline.py info              # stats sur les tables finales
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated

import dlt
import duckdb
import typer
from rich.console import Console
from rich.table import Table

from src.le_grand_livre_des_recettes.pipeline.sources.mit_recipes import mit_recipes_source
from src.le_grand_livre_des_recettes.pipeline.sources.kaggle_recipes import kaggle_recipes_source

app = typer.Typer(help="🍽️  Recipes Data Pipeline — dlt + DuckDB")
console = Console()

# ---------------------------------------------------------------------------
# Chemins
# ---------------------------------------------------------------------------
DATA_DIR    = Path("data")
OUTPUTS_DIR = DATA_DIR / "outputs"
DB_PATH     = OUTPUTS_DIR / "recipes_catalog.duckdb"
SQL_DIR     = Path("sql")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@app.command()
def run(
    dest: Annotated[str, typer.Option(help="Destination : 'duckdb' ou 'delta'")] = "duckdb",
) -> None:
    """Lance le pipeline complet : ingestion dlt → transformations SQL."""
    console.rule("[bold blue]🍽️  Recipes Pipeline — Full Run")
    t0 = time.perf_counter()

    ingest(dest=dest)
    transform()

    elapsed = time.perf_counter() - t0
    console.print(f"\n[green]✅ Pipeline terminé en {elapsed:.1f}s[/green]")


@app.command()
def ingest(
    dest: Annotated[str, typer.Option(help="Destination : 'duckdb' ou 'delta'")] = "duckdb",
) -> None:
    """
    Étape 1 — Ingestion dlt uniquement.
    Charge les sources raw vers les tables staging.
    """
    console.rule("[bold]Étape 1 — Ingestion dlt (sources → staging)")

    destination = _build_destination(dest)

    pipeline = dlt.pipeline(
        pipeline_name="recipes_pipeline",
        destination=destination,
        dataset_name="recipes",
        progress="log",        # <-- LIGNE MODIFIÉE : on remplace "rich" par "log" (ou on efface la ligne)
    )

    # Extraction des deux sources en parallèle
    sources = [
        mit_recipes_source(),
        kaggle_recipes_source(),
    ]

    with console.status("Extraction + chargement en cours..."):
        load_info = pipeline.run(sources, loader_file_format="parquet")

    console.print(load_info)
    console.print("[green]✅ Ingestion terminée[/green]")


@app.command()
def transform() -> None:
    """
    Étape 2 — Transformations SQL uniquement (sur staging DuckDB existant).
    Jointures, enrichissement et création des 3 tables finales.
    """
    console.rule("[bold]Étape 2 — Transformations SQL (jointures + enrichissement)")

    sql_files = sorted(SQL_DIR.glob("*.sql"))

    if not sql_files:
        console.print("[red]Aucun fichier SQL trouvé dans sql/[/red]")
        raise typer.Exit(1)

    with duckdb.connect(str(DB_PATH)) as con:
        temp_dir = OUTPUTS_DIR / "tmp"
        temp_dir.mkdir(exist_ok=True)
        
        # .as_posix() force les forward slashes (ex: "data/outputs/tmp")
        con.execute(f"SET temp_directory='{temp_dir.as_posix()}'")

        for sql_file in sql_files:
            console.print(f"  → Exécution de [cyan]{sql_file.name}[/cyan]")
            t0 = time.perf_counter()
            sql_text = sql_file.read_text(encoding="utf-8")
            statements = [s.strip() for s in sql_text.split(";") if s.strip()]
            for stmt in statements:
                con.execute(stmt)
            elapsed = time.perf_counter() - t0
            console.print(f"    [dim]terminé en {elapsed:.1f}s[/dim]")

    console.print("[green]✅ Transformations terminées[/green]")


@app.command()
def info() -> None:
    """Affiche les statistiques des tables finales."""
    console.rule("[bold]Stats des tables finales")

    if not DB_PATH.exists():
        console.print(f"[red]Base introuvable : {DB_PATH}[/red]")
        raise typer.Exit(1)

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute("""
            SELECT
                'recipes_main'             AS table_name,
                COUNT(*)                   AS rows,
                COUNT(image_url)           AS with_image,
                COUNT(nutri_score)         AS with_nutri_score
            FROM recipes.recipes_main
            UNION ALL
            SELECT 'ingredients_index',
                COUNT(*), NULL, NULL
            FROM recipes.ingredients_index
            UNION ALL
            SELECT 'recipes_nutrition_detail',
                COUNT(*), NULL, NULL
            FROM recipes.recipes_nutrition_detail
        """).fetchall()

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Table")
    table.add_column("Rows", justify="right")
    table.add_column("With image", justify="right")
    table.add_column("With nutri-score", justify="right")

    for row in rows:
        table.add_row(
            row[0],
            f"{row[1]:,}",
            f"{row[2]:,}" if row[2] is not None else "—",
            f"{row[3]:,}" if row[3] is not None else "—",
        )

    console.print(table)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_destination(dest: str):
    """Construit la destination dlt selon l'option --dest."""
    if dest == "duckdb":
        # S'assurer que le dossier parent existe pour DuckDB
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        return dlt.destinations.duckdb(
            credentials=str(DB_PATH)
        )
    if dest == "delta":
        # S'assurer que le dossier parent existe pour le filesystem
        (OUTPUTS_DIR / "delta").mkdir(parents=True, exist_ok=True)
        return dlt.destinations.filesystem(
            bucket_url=str(OUTPUTS_DIR / "delta"),
            destination_name="filesystem",
        )
    console.print(f"[red]Destination inconnue : '{dest}'. Utiliser 'duckdb' ou 'delta'.[/red]")
    raise typer.Exit(1)


if __name__ == "__main__":
    app()

"""
Entrypoint du pipeline recipes.

Architecture en deux phases :
- Phase 1 : Ingestion via dlt (raw vers staging au format Delta Lake).
- Phase 2 : Transformation via PySpark (staging Delta vers outputs Delta).

Usage :
    python run_pipeline.py run
    python run_pipeline.py ingest
    python run_pipeline.py transform
    python run_pipeline.py info
"""

from __future__ import annotations

import time
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from le_grand_livre_des_recettes.pipeline import config as cfg
from le_grand_livre_des_recettes.pipeline.ingest import run_ingestion
from le_grand_livre_des_recettes.pipeline.spark_session import get_or_create_spark
from le_grand_livre_des_recettes.pipeline.transformers.assemble import assemble
from le_grand_livre_des_recettes.pipeline.transformers.enrich import write_final_tables

app = typer.Typer(help="Recipes Data Pipeline - dlt and PySpark")
console = Console()


@app.command()
def run(
    master: Annotated[str | None, typer.Option(help="URL du master Spark")] = None,
) -> None:
    """Exécute le pipeline complet (ingestion dlt et transformations PySpark)."""
    console.rule("[bold blue]Recipes Pipeline - Full Run")
    t0: float = time.perf_counter()

    ingest()
    transform(master=master)

    console.print(f"\n[green]Pipeline termine en {time.perf_counter() - t0:.1f}s[/green]")


@app.command()
def ingest() -> None:
    """Exécute la phase 1 : Ingestion des données brutes vers le staging via dlt."""
    console.rule("[bold]Phase 1 - Ingestion dlt")
    cfg.STAGING_DIR.mkdir(parents=True, exist_ok=True)

    t0: float = time.perf_counter()
    run_ingestion()
    console.print(f"\n[green]Ingestion dlt terminee en {time.perf_counter() - t0:.1f}s[/green]")


@app.command()
def transform(
    master: Annotated[str | None, typer.Option(help="URL du master Spark")] = None,
) -> None:
    """Exécute la phase 2 : Transformations PySpark et génération des tables finales."""
    console.rule("[bold]Phase 2 - Transformation PySpark")
    cfg.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = get_or_create_spark(master=master)
    console.print(f"Spark master : [cyan]{spark.sparkContext.master}[/cyan]")

    t0: float = time.perf_counter()

    console.print("Assemblage des DataFrames...")
    df_assembled = assemble(spark)

    console.print("Enrichissement et ecriture des tables finales...")
    write_final_tables(df_assembled)

    console.print(f"\n[green]Transformations PySpark terminees en {time.perf_counter() - t0:.1f}s[/green]")


@app.command()
def info(
    master: Annotated[str | None, typer.Option(help="URL du master Spark")] = None,
) -> None:
    """Affiche les statistiques des tables finales."""
    console.rule("[bold]Stats des tables finales")

    spark = get_or_create_spark(master=master)

    try:
        df_main = spark.read.format("delta").load(cfg.OUT_RECIPES_MAIN)
        df_index = spark.read.format("delta").load(cfg.OUT_INGREDIENTS_INDEX)
        df_nutr = spark.read.format("delta").load(cfg.OUT_NUTRITION_DETAIL)
    except Exception as exc:
        console.print(f"[red]Erreur de lecture des tables finales : {exc}[/red]")
        raise typer.Exit(1)

    total_recipes: int = df_main.count()
    with_image: int = df_main.filter("image_url IS NOT NULL").count()
    with_nutri: int = df_main.filter("nutri_score IS NOT NULL").count()
    with_mit_energy: int = df_main.filter("mit_energy_kcal IS NOT NULL").count()
    index_rows: int = df_index.count()
    nutr_rows: int = df_nutr.count()

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Table")
    table.add_column("Lignes", justify="right")
    table.add_column("Avec image", justify="right")
    table.add_column("Avec nutri-score", justify="right")

    table.add_row(
        "recipes_main",
        f"{total_recipes:,}",
        f"{with_image:,}",
        f"{with_nutri:,} (mit_energy : {with_mit_energy:,})",
    )
    table.add_row("ingredients_index", f"{index_rows:,}", "-", "-")
    table.add_row("recipes_nutrition_detail", f"{nutr_rows:,}", "-", "-")

    console.print(table)


if __name__ == "__main__":
    app()
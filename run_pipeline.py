"""
Entrypoint du pipeline recipes.

Architecture à deux phases :

    Phase 1 — Ingestion (dlt)
        data/raw/*.json + *.csv
          ↓ dlt resources (normalisation Python, streaming ijson/csv)
          ↓ destination filesystem
        data/staging/*.parquet

    Phase 2 — Transformation (PySpark)
        data/staging/*.parquet
          ↓ jointures LEFT JOIN + enrichissement
        data/outputs/parquets/
            recipes_main/              (partitionné par nutri_score)
            ingredients_index/         (une ligne par recette × ingrédient)
            recipes_nutrition_detail/  (macronutriments kcal/100g)

Usage (dans le container spark-master ou en local) :

    python run_pipeline.py run              # pipeline complet
    python run_pipeline.py ingest           # Phase 1 dlt uniquement
    python run_pipeline.py transform        # Phase 2 PySpark uniquement
    python run_pipeline.py info             # stats des tables finales
    python run_pipeline.py run --master spark://spark-master:7077

Variables d'environnement :
    SPARK_MASTER_URL   URL du master Spark (défaut : local[*])
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

app     = typer.Typer(help="🍽️  Recipes Data Pipeline — dlt + PySpark")
console = Console()


@app.command()
def run(
    master: Annotated[str | None, typer.Option(help="URL du master Spark")] = None,
) -> None:
    """Lance le pipeline complet : ingestion dlt → transformations PySpark."""
    console.rule("[bold blue]🍽️  Recipes Pipeline — Full Run (dlt + PySpark)")
    t0 = time.perf_counter()

    ingest()
    transform(master=master)

    console.print(f"\n[green]✅ Pipeline terminé en {time.perf_counter() - t0:.1f}s[/green]")


@app.command()
def ingest() -> None:
    """
    Phase 1 — Ingestion via dlt.

    Lit data/raw/ (JSON + CSV), normalise en Python et écrit des Parquets
    dans data/staging/ via la destination filesystem.

    Tables staging créées : layer1/ layer2/ det_ingrs/ nutrition/ kaggle/
    """
    console.rule("[bold]Phase 1 — Ingestion (dlt : raw → staging Parquet)")
    cfg.STAGING_DIR.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    run_ingestion()
    console.print(f"\n[green]✅ Ingestion dlt terminée en {time.perf_counter() - t0:.1f}s[/green]")


@app.command()
def transform(
    master: Annotated[str | None, typer.Option(help="URL du master Spark")] = None,
) -> None:
    """
    Phase 2 — Transformation via PySpark.

    Lit data/staging/, effectue les jointures et enrichissements,
    puis écrit 3 tables finales dans data/outputs/parquets/.
    """
    console.rule("[bold]Phase 2 — Transformation (PySpark : staging → tables finales)")
    cfg.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = get_or_create_spark(master=master)
    console.print(f"  Spark master : [cyan]{spark.sparkContext.master}[/cyan]")

    t0 = time.perf_counter()

    console.print("  → Assemblage des DataFrames...")
    df_assembled = assemble(spark)

    console.print("  → Enrichissement et écriture des tables finales...")
    write_final_tables(df_assembled)

    console.print(f"\n[green]✅ Transformations PySpark terminées en {time.perf_counter() - t0:.1f}s[/green]")


@app.command()
def info(
    master: Annotated[str | None, typer.Option(help="URL du master Spark")] = None,
) -> None:
    """Affiche les statistiques des 3 tables finales."""
    console.rule("[bold]Stats des tables finales")

    spark = get_or_create_spark(master=master)

    try:
        df_main  = spark.read.parquet(cfg.OUT_RECIPES_MAIN)
        df_index = spark.read.parquet(cfg.OUT_INGREDIENTS_INDEX)
        df_nutr  = spark.read.parquet(cfg.OUT_NUTRITION_DETAIL)
    except Exception as exc:
        console.print(f"[red]Impossible de lire les tables finales : {exc}[/red]")
        console.print("[yellow]Avez-vous lancé 'run' ou 'transform' au préalable ?[/yellow]")
        raise typer.Exit(1)

    total_recipes   = df_main.count()
    with_image      = df_main.filter("image_url IS NOT NULL").count()
    with_nutri      = df_main.filter("nutri_score IS NOT NULL").count()
    with_mit_energy = df_main.filter("mit_energy_kcal IS NOT NULL").count()
    index_rows      = df_index.count()
    nutr_rows       = df_nutr.count()

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Table")
    table.add_column("Lignes",            justify="right")
    table.add_column("Avec image",        justify="right")
    table.add_column("Avec nutri-score",  justify="right")

    table.add_row(
        "recipes_main",
        f"{total_recipes:,}",
        f"{with_image:,}",
        f"{with_nutri:,}  (mit_energy : {with_mit_energy:,})",
    )
    table.add_row("ingredients_index",        f"{index_rows:,}", "—", "—")
    table.add_row("recipes_nutrition_detail", f"{nutr_rows:,}",  "—", "—")

    console.print(table)


if __name__ == "__main__":
    app()

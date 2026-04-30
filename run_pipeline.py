"""
Entrypoint du pipeline recipes (PySpark).

Remplace l'ancienne implémentation dlt + DuckDB par un pipeline PySpark pur.
La session Spark est fournie par le cluster Docker (docker-compose up),
ou créée en mode local[*] si la variable SPARK_MASTER_URL n'est pas définie.

Usage :
    python run_pipeline.py run              # pipeline complet (ingest + transform)
    python run_pipeline.py run --master spark://spark-master:7077
    python run_pipeline.py ingest           # Phase 1 : staging Parquet uniquement
    python run_pipeline.py transform        # Phase 2 : jointures + tables finales
    python run_pipeline.py info             # stats sur les tables finales

Variables d'environnement :
    SPARK_MASTER_URL   URL du master Spark (défaut : local[*])
                       Exemple : spark://spark-master:7077

Architecture du pipeline :
    Phase 1 — Ingestion (ingest)
      Lecture des fichiers sources (JSON + CSV) → écriture Parquet staging.
      Identique à la Phase 1 du notebook Databricks.

    Phase 2 — Transformation (transform)
      Lecture des Parquets staging → jointures → enrichissement → 3 tables finales.
      Identique aux SQL 01_assemble.sql + 02_final_tables.sql.

Tables finales produites dans data/outputs/parquets/ :
    recipes_main/              → une ligne par recette
    ingredients_index/         → une ligne par (recette, ingrédient)
    recipes_nutrition_detail/  → détail nutritionnel par recette
"""

from __future__ import annotations

import time
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from src.le_grand_livre_des_recettes.pipeline import config as cfg
from src.le_grand_livre_des_recettes.pipeline.spark_session import get_or_create_spark
from src.le_grand_livre_des_recettes.pipeline.sources.mit_recipes import write_mit_staging
from src.le_grand_livre_des_recettes.pipeline.sources.kaggle_recipes import write_kaggle_staging
from src.le_grand_livre_des_recettes.pipeline.transformers.assemble import assemble
from src.le_grand_livre_des_recettes.pipeline.transformers.enrich import write_final_tables

app     = typer.Typer(help="🍽️  Recipes Data Pipeline — PySpark")
console = Console()


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@app.command()
def run(
    master: Annotated[str | None, typer.Option(help="URL du master Spark")] = None,
) -> None:
    """Lance le pipeline complet : ingestion (staging) → transformations (tables finales)."""
    console.rule("[bold blue]🍽️  Recipes Pipeline — Full Run (PySpark)")
    t0 = time.perf_counter()

    ingest(master=master)
    transform(master=master)

    elapsed = time.perf_counter() - t0
    console.print(f"\n[green]✅ Pipeline terminé en {elapsed:.1f}s[/green]")


@app.command()
def ingest(
    master: Annotated[str | None, typer.Option(help="URL du master Spark")] = None,
) -> None:
    """
    Phase 1 — Ingestion : lit les fichiers sources et écrit les Parquets staging.

    Fichiers lus depuis data/raw/ :
      layer1.json, layer2+.json, det_ingrs.json,
      recipes_with_nutritional_info.json, RAW_recipes.csv

    Parquets écrits dans data/staging/.
    """
    console.rule("[bold]Phase 1 — Ingestion (sources → staging Parquet)")

    # Création des dossiers de sortie
    cfg.STAGING_DIR.mkdir(parents=True, exist_ok=True)

    spark = get_or_create_spark(master=master)
    console.print(f"  Spark master : [cyan]{spark.sparkContext.master}[/cyan]")

    t0 = time.perf_counter()
    write_mit_staging(spark)
    write_kaggle_staging(spark)
    elapsed = time.perf_counter() - t0

    console.print(f"\n[green]✅ Ingestion terminée en {elapsed:.1f}s[/green]")


@app.command()
def transform(
    master: Annotated[str | None, typer.Option(help="URL du master Spark")] = None,
) -> None:
    """
    Phase 2 — Transformation : jointures + enrichissement → 3 tables finales Parquet.

    Lit depuis data/staging/.
    Écrit dans data/outputs/parquets/.
    """
    console.rule("[bold]Phase 2 — Transformation (staging → tables finales)")

    # Création des dossiers de sortie
    cfg.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (cfg.OUTPUT_DIR.parent / "tmp").mkdir(parents=True, exist_ok=True)

    spark = get_or_create_spark(master=master)
    console.print(f"  Spark master : [cyan]{spark.sparkContext.master}[/cyan]")

    t0 = time.perf_counter()

    console.print("  → Assemblage des DataFrames...")
    df_assembled = assemble(spark)

    console.print("  → Écriture des tables finales...")
    write_final_tables(df_assembled)

    elapsed = time.perf_counter() - t0
    console.print(f"\n[green]✅ Transformations terminées en {elapsed:.1f}s[/green]")


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

    # Calcul des stats
    total_recipes   = df_main.count()
    with_image      = df_main.filter("image_url IS NOT NULL").count()
    with_nutri      = df_main.filter("nutri_score IS NOT NULL").count()
    with_mit_energy = df_main.filter("mit_energy_kcal IS NOT NULL").count()
    index_rows      = df_index.count()
    nutr_rows       = df_nutr.count()

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
    table.add_row("ingredients_index",        f"{index_rows:,}", "—", "—")
    table.add_row("recipes_nutrition_detail", f"{nutr_rows:,}",  "—", "—")

    console.print(table)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()

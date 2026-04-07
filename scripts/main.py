"""
Recipe Search API — Production-ready
Stack : FastAPI + DuckDB (in-process)
Run   :
Run   : uvicorn main:app --host 127.0.0.1 --port 8000 --workers 1
Note  : workers=1 car DuckDB in-memory n'est pas partageable entre process.
        Pour le multi-process, utiliser DuckDB en mode fichier (.db) avec
        des connexions read-only par worker.
"""

from contextlib import asynccontextmanager
from typing import Optional
import time

import duckdb
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_PATH = "../data/recipes_parquets"
DB_MEMORY_LIMIT = "1.5GB"
DB_THREADS = 4


# ---------------------------------------------------------------------------
# Modèles de réponse
# ---------------------------------------------------------------------------
class RecipeSummary(BaseModel):
    recipe_id: str
    title: str
    cook_minutes: Optional[int]
    nutri_score: Optional[str]
    image_url: Optional[str]
    score: Optional[float] = None


class RecipeDetail(RecipeSummary):
    cook_time_category: Optional[str]
    energy_kcal: Optional[float]
    ingredients_raw: Optional[list]
    instructions_text: Optional[str]
    fat_g: Optional[float]
    protein_g: Optional[float]
    salt_g: Optional[float]
    saturates_g: Optional[float]
    sugars_g: Optional[float]


class SearchResponse(BaseModel):
    results: list[RecipeSummary]
    count: int
    elapsed_ms: float


# ---------------------------------------------------------------------------
# Lifecycle : init DuckDB au démarrage, fermeture propre à l'arrêt
# ---------------------------------------------------------------------------
con: duckdb.DuckDBPyConnection = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global con
    print("🚀 Initialisation DuckDB...")
    t0 = time.perf_counter()

    con = duckdb.connect(database=":memory:", config={
        "threads": DB_THREADS,
        "memory_limit": DB_MEMORY_LIMIT,
    })

    # Extensions
    con.execute("INSTALL delta; LOAD delta;")
    con.execute("INSTALL fts;   LOAD fts;")

    # Tables en RAM (chargement unique au démarrage)
    con.execute(f"""
        CREATE TABLE t_main AS
            SELECT * FROM delta_scan('{DATA_PATH}/recipes_main');

        CREATE TABLE t_nutrition AS
            SELECT * FROM delta_scan('{DATA_PATH}/recipes_nutrition_detail');

        CREATE TABLE t_index AS
            SELECT * FROM delta_scan('{DATA_PATH}/ingredients_index');
    """)

    # Vue master jointe — energy_kcal est dans recipes_main, pas nutrition
    con.execute("""
        CREATE TABLE t_master AS
        SELECT
            m.recipe_id,
            m.title,
            m.description,
            m.instructions_text,
            m.ingredients_raw,
            m.ingredients_validated,
            m.cook_minutes,
            m.cook_time_category,
            m.image_url,
            m.energy_kcal,
            m.nutri_score,
            m.tags,
            n.fat_g,
            n.protein_g,
            n.salt_g,
            n.saturates_g,
            n.sugars_g
        FROM t_main m
        LEFT JOIN t_nutrition n USING (recipe_id);
    """)

    # Index full-text
    con.execute("""
        PRAGMA create_fts_index(
            't_master', 'recipe_id',
            'title', 'ingredients_raw',
            stemmer = 'english',
            overwrite = 1
        );
    """)

    elapsed = (time.perf_counter() - t0) * 1000
    count = con.execute("SELECT COUNT(*) FROM t_master").fetchone()[0]
    print(f"✅ {count:,} recettes prêtes en {elapsed:.0f} ms")

    yield  # API en service

    con.close()
    print("👋 DuckDB fermé proprement")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Recipe Search API",
    version="1.0.0",
    lifespan=lifespan,
)


def _rows_to_summaries(rows, cols) -> list[RecipeSummary]:
    return [RecipeSummary(**dict(zip(cols, r))) for r in rows]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    count = con.execute("SELECT COUNT(*) FROM t_master").fetchone()[0]
    return {"status": "ok", "recipes": count}


@app.get("/search/name", response_model=SearchResponse)
def search_by_name(
    q: str = Query(..., min_length=2, description="Terme recherché dans le titre"),
    limit: int = Query(20, ge=1, le=100),
):
    t0 = time.perf_counter()
    rows = con.execute("""
        SELECT recipe_id, title, cook_minutes, nutri_score, image_url,
               fts_main_t_master.match_bm25(recipe_id, ?, fields := 'title') AS score
        FROM t_master
        WHERE score IS NOT NULL
        ORDER BY score DESC
        LIMIT ?
    """, [q, limit]).fetchall()

    cols = ["recipe_id", "title", "cook_minutes", "nutri_score", "image_url", "score"]
    return SearchResponse(
        results=_rows_to_summaries(rows, cols),
        count=len(rows),
        elapsed_ms=round((time.perf_counter() - t0) * 1000, 2),
    )


@app.get("/search/ingredient", response_model=SearchResponse)
def search_by_ingredient(
    q: str = Query(..., min_length=2, description="Ingrédient recherché"),
    limit: int = Query(20, ge=1, le=100),
):
    t0 = time.perf_counter()
    rows = con.execute("""
        SELECT recipe_id, title, cook_minutes, nutri_score, image_url,
               fts_main_t_master.match_bm25(recipe_id, ?, fields := 'ingredients_raw') AS score
        FROM t_master
        WHERE score IS NOT NULL
        ORDER BY score DESC
        LIMIT ?
    """, [q, limit]).fetchall()

    cols = ["recipe_id", "title", "cook_minutes", "nutri_score", "image_url", "score"]
    return SearchResponse(
        results=_rows_to_summaries(rows, cols),
        count=len(rows),
        elapsed_ms=round((time.perf_counter() - t0) * 1000, 2),
    )


@app.get("/search/advanced", response_model=SearchResponse)
def search_advanced(
    name: Optional[str] = Query(None, min_length=2),
    ingredient: Optional[str] = Query(None, min_length=2),
    max_time: Optional[int] = Query(None, ge=1, le=1440),
    nutri_score: Optional[str] = Query(None, pattern="^[A-Ea-e]$"),
    limit: int = Query(20, ge=1, le=100),
):
    t0 = time.perf_counter()
    conditions = ["1=1"]
    params = []

    if max_time:
        conditions.append("cook_minutes <= ?")
        params.append(max_time)

    if nutri_score:
        conditions.append("nutri_score = ?")
        params.append(nutri_score.upper())

    fts_fragment = ""
    if name or ingredient:
        fts_text = " ".join(filter(None, [name, ingredient]))
        fields = "title" if name and not ingredient else \
                 "ingredients_raw" if ingredient and not name else \
                 "title, ingredients_raw"
        fts_fragment = f"""
            AND fts_main_t_master.match_bm25(recipe_id, ?, fields := '{fields}') IS NOT NULL
        """
        params = [fts_text] + params  # FTS param en tête

    where = " AND ".join(conditions)
    params.append(limit)

    rows = con.execute(f"""
        SELECT recipe_id, title, cook_minutes, nutri_score, image_url, NULL AS score
        FROM t_master
        WHERE {where} {fts_fragment}
        LIMIT ?
    """, params).fetchall()

    cols = ["recipe_id", "title", "cook_minutes", "nutri_score", "image_url", "score"]
    return SearchResponse(
        results=_rows_to_summaries(rows, cols),
        count=len(rows),
        elapsed_ms=round((time.perf_counter() - t0) * 1000, 2),
    )


@app.get("/recipe/{recipe_id}", response_model=RecipeDetail)
def get_recipe(recipe_id: str):
    row = con.execute("""
        SELECT recipe_id, title, cook_minutes, cook_time_category,
               nutri_score, image_url, ingredients_raw, instructions_text,
               energy_kcal, fat_g, protein_g, salt_g, saturates_g, sugars_g
        FROM t_master WHERE recipe_id = ? LIMIT 1
    """, [recipe_id]).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Recette introuvable")

    cols = ["recipe_id", "title", "cook_minutes", "cook_time_category",
            "nutri_score", "image_url", "ingredients_raw", "instructions_text",
            "energy_kcal", "fat_g", "protein_g", "salt_g", "saturates_g", "sugars_g"]
    return RecipeDetail(**dict(zip(cols, row)))
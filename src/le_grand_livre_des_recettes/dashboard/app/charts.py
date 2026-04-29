"""
Génération des graphiques Plotly.
Toutes les fonctions intègrent le filtrage croisé (cross-filtering).
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from .config import PALETTE, NUTRI_COLORS, PLOT_LAYOUT
from .data import con


def _exec(query: str, params: list):
    """Wrapper con.execute qui évite le bug DuckDB avec params=[] vide, et gère le multi-threading."""
    # Création d'un curseur local pour la sécurité des threads
    cursor = con.cursor()

    if params:
        return cursor.execute(query, params)
    return cursor.execute(query)

def _build_where(
        nutri_scores: list[str] | None = None,
        cook_cats: list[str] | None = None,
        kcal_min: float | None = None,
        kcal_max: float | None = None,
        table_prefix: str = "",
        base_clauses: list[str] | None = None,
) -> tuple[str, list]:
    """
    Génère dynamiquement la clause WHERE et ses paramètres.
    """
    clauses = base_clauses.copy() if base_clauses else []
    params = []
    p = table_prefix

    if nutri_scores:
        clauses.append(f"{p}nutri_score IN ({','.join(['?'] * len(nutri_scores))})")
        params.extend(nutri_scores)

    if cook_cats:
        clauses.append(f"{p}cook_time_category IN ({','.join(['?'] * len(cook_cats))})")
        params.extend(cook_cats)

    if kcal_min is not None:
        clauses.append(f"{p}mit_energy_kcal >= ?")
        params.append(kcal_min)

    if kcal_max is not None:
        clauses.append(f"{p}mit_energy_kcal <= ?")
        params.append(kcal_max)

    where_str = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where_str, params


def kcal_histogram(nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None) -> go.Figure:
    where_str, params = _build_where(
        nutri_scores, cook_cats, kcal_min, kcal_max,
        base_clauses=["mit_energy_kcal IS NOT NULL", "mit_energy_kcal < 3500"]
    )

    df = _exec(f"SELECT mit_energy_kcal AS energy_kcal FROM recipes.recipes_main {where_str}", params).df()

    fig = px.histogram(df, x="energy_kcal", nbins=60, color_discrete_sequence=[PALETTE["accent1"]])
    fig.update_traces(marker_line_width=0.5, marker_line_color="white")
    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text=f"Distribution énergie (kcal) — {len(df):,} recettes".replace(",", "\u202f"), x=0.5,
                   font_size=13),
        xaxis_title="kcal", yaxis_title="recettes", clickmode="event+select",
    )
    fig.update_xaxes(showgrid=False, zeroline=False, tickfont_size=10)
    fig.update_yaxes(showgrid=True, gridcolor=PALETTE["border"], zeroline=False, tickfont_size=10)
    return fig


def nutri_pie(nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None) -> go.Figure:
    where_str, params = _build_where(nutri_scores, cook_cats, kcal_min, kcal_max, table_prefix="m.")
    joins = "JOIN recipes.recipes_main m ON n.recipe_id = m.recipe_id" if where_str else ""

    query = f"""
        SELECT 
            SUM(n.fat_g) AS "Matières grasses",
            SUM(n.protein_g) AS "Protéines",
            SUM(n.salt_g) AS "Sel",
            SUM(n.saturates_g) AS "Graisses saturées",
            SUM(n.sugars_g) AS "Sucres"
        FROM recipes.recipes_nutrition_detail n
        {joins} {where_str}
    """
    df = _exec(query, params).df()

    if df.empty or df.iloc[0].isna().all():
        return go.Figure().update_layout(**PLOT_LAYOUT, title=dict(text="Données indisponibles", x=0.5, font_size=13))

    colors = [PALETTE["accent1"], PALETTE["accent2"], PALETTE["muted"], PALETTE["accent3"], PALETTE["accent4"]]
    fig = go.Figure(go.Pie(
        labels=df.columns.tolist(), values=df.iloc[0].tolist(), hole=0.52,
        marker=dict(colors=colors, line=dict(color="white", width=2)),
        textinfo="label+percent", textposition="outside", textfont_size=10,
    ))
    fig.update_layout(**PLOT_LAYOUT, title=dict(text="Répartition nutritionnelle totale", x=0.5, font_size=13))
    return fig


def nutri_bar(nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None) -> go.Figure:
    where_str, params = _build_where(nutri_scores, cook_cats, kcal_min, kcal_max,
                                     base_clauses=["nutri_score IS NOT NULL"])

    df = _exec(f"""
        SELECT nutri_score AS score, COUNT(*) AS count
        FROM recipes.recipes_main {where_str}
        GROUP BY score ORDER BY score
    """, params).df()

    bar_colors = [
        NUTRI_COLORS.get(s, PALETTE["muted"]) if (not nutri_scores or s in nutri_scores) else PALETTE["border"]
        for s in df["score"]
    ]

    fig = go.Figure(go.Bar(
        x=df["count"], y=df["score"], orientation="h",
        marker_color=bar_colors, customdata=df["score"],
        hovertemplate="<b>Nutri-Score %{y}</b><br>%{x:,} recettes<extra></extra>",
    ))
    fig.update_layout(
        **PLOT_LAYOUT, title=dict(text="Répartition Nutri-Score", x=0.5, font_size=13),
        clickmode="event+select", yaxis=dict(categoryorder="category ascending")
    )
    fig.update_xaxes(showgrid=True, gridcolor=PALETTE["border"])
    fig.update_yaxes(showgrid=False, categoryorder="category ascending")
    return fig


def cook_time_chart(nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None) -> go.Figure:
    where_str, params = _build_where(
        nutri_scores, cook_cats, kcal_min, kcal_max,
        base_clauses=["cook_time_category IN ('rapide', 'moyen', 'long')"]
    )

    df = _exec(f"""
        SELECT cook_time_category, COUNT(*) AS count
        FROM recipes.recipes_main {where_str}
        GROUP BY cook_time_category
    """, params).df()

    ct = df.set_index("cook_time_category").reindex(["rapide", "moyen", "long"]).reset_index()
    base_colors = [PALETTE["accent2"], PALETTE["accent4"], PALETTE["accent3"]]

    bar_colors = [
        color if (not cook_cats or cat in cook_cats) else PALETTE["border"]
        for cat, color in zip(["rapide", "moyen", "long"], base_colors)
    ]

    fig = go.Figure(go.Bar(
        x=ct["cook_time_category"], y=ct["count"],
        marker_color=bar_colors,
        text=ct["count"].apply(lambda x: f"{int(x):,}".replace(",", "\u202f") if pd.notna(x) else "0"),
        textposition="outside", textfont_size=10, customdata=ct["cook_time_category"],
        hovertemplate="<b>%{x}</b><br>%{y:,} recettes<extra></extra>",
    ))
    fig.update_layout(
        **PLOT_LAYOUT, title=dict(text="Temps de cuisson", x=0.5, font_size=13),
        clickmode="event+select", yaxis=dict(showgrid=True, gridcolor=PALETTE["border"])
    )
    return fig


def cook_time_curve(nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None) -> go.Figure:
    where_str, params = _build_where(
        nutri_scores, cook_cats, kcal_min, kcal_max,
        base_clauses=["cook_minutes IS NOT NULL", "cook_minutes BETWEEN 1 AND 300"]
    )

    df = _exec(f"""
        SELECT FLOOR(cook_minutes / 5) * 5 AS bucket, COUNT(*) AS count
        FROM recipes.recipes_main {where_str}
        GROUP BY bucket ORDER BY bucket
    """, params).df()

    fig = go.Figure(go.Scatter(
        x=df["bucket"], y=df["count"], mode="lines",
        line=dict(color=PALETTE["accent4"], width=2.5, shape="spline", smoothing=1.2),
        fill="tozeroy", fillcolor="rgba(74, 111, 165, 0.13)",
        hovertemplate="%{x} min — %{y:,} recettes<extra></extra>",
    ))
    fig.update_layout(
        **PLOT_LAYOUT, title=dict(text="Distribution des temps de cuisson (min)", x=0.5, font_size=13),
        xaxis_title="minutes", yaxis_title="recettes",
        xaxis=dict(showgrid=False, zeroline=False, tickfont_size=10),
        yaxis=dict(showgrid=True, gridcolor=PALETTE["border"], zeroline=False, tickfont_size=10),
    )
    return fig


def scatter_saturates_sugars(nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None) -> go.Figure:
    base_clauses = [
        "n.saturates_g IS NOT NULL", "n.sugars_g IS NOT NULL",
        "m.nutri_score IS NOT NULL", "n.saturates_g BETWEEN 0 AND 60", "n.sugars_g BETWEEN 0 AND 100"
    ]
    where_str, params = _build_where(nutri_scores, cook_cats, kcal_min, kcal_max, table_prefix="m.",
                                     base_clauses=base_clauses)

    df = _exec(f"""
        SELECT n.saturates_g, n.sugars_g, COALESCE(m.nutri_score, '?') AS nutri_score, m.title
        FROM recipes.recipes_nutrition_detail n
        JOIN recipes.recipes_main m ON n.recipe_id = m.recipe_id
        {where_str} USING SAMPLE 2000 ROWS
    """, params).df()

    fig = go.Figure()
    for score in ["A", "B", "C", "D", "E", "?"]:
        sub = df[df["nutri_score"] == score]
        if not sub.empty:
            fig.add_trace(go.Scatter(
                x=sub["saturates_g"], y=sub["sugars_g"], mode="markers", name=f"Score {score}", text=sub["title"],
                marker=dict(color=NUTRI_COLORS.get(score, PALETTE["muted"]), size=5, opacity=0.6),
                hovertemplate="<b>%{text}</b><br>Nutri-Score %{name}<br>Graisses saturées: %{x:.1f}g<br>Sucres: %{y:.1f}g<extra></extra>"
            ))
    # 1. On applique le thème global
    fig.update_layout(**PLOT_LAYOUT)

    # 2. On surcharge avec les paramètres spécifiques au scatter plot
    fig.update_layout(
        title=dict(text="Sucres vs Graisses saturées (axes Nutri-Score)", x=0.5, font_size=13),
        xaxis_title="graisses saturées (g)", yaxis_title="sucres (g)",
        showlegend=True,  # Surcharge le 'showlegend=False' du PLOT_LAYOUT
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis = dict(showgrid=True, gridcolor=PALETTE["border"], zeroline=False, tickfont_size=10),
        yaxis = dict(showgrid=True, gridcolor=PALETTE["border"], zeroline=False, tickfont_size=10),
    )
    return fig


def _generic_top_chart(sql_field: str, color: str, title: str, nutri_scores, cook_cats, kcal_min, kcal_max,
                       top_n: int) -> go.Figure:
    """Helper générique pour les tops (ingrédients et tags) afin d'éviter la répétition."""
    where_str, params = _build_where(nutri_scores, cook_cats, kcal_min, kcal_max)
    params.append(top_n)

    df = _exec(f"""
        SELECT item, COUNT(*) AS freq
        FROM (
            SELECT UNNEST({sql_field}) AS item
            FROM recipes.recipes_main {where_str}
        ) sub
        WHERE item IS NOT NULL AND LENGTH(TRIM(item)) > 1
        GROUP BY item ORDER BY freq DESC LIMIT ?
    """, params).df()

    if df.empty:
        return go.Figure().update_layout(**PLOT_LAYOUT,
                                         title=dict(text=f"Aucun {title.lower()} disponible", x=0.5, font_size=13))

    fig = go.Figure(go.Bar(
        x=df["freq"], y=df["item"], orientation="h", marker_color=color,
        hovertemplate="<b>%{y}</b><br>%{x:,} recettes<extra></extra>"
    ))
    fig.update_layout(
        **PLOT_LAYOUT, title=dict(text=f"Top {top_n} {title}", x=0.5, font_size=13),
        xaxis_title="recettes", yaxis_title="",
        yaxis=dict(autorange="reversed", tickfont_size=10),
        xaxis=dict(showgrid=True, gridcolor=PALETTE["border"]),
    )
    return fig


def ingredients_top_chart(nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None, top_n=15) -> go.Figure:
    return _generic_top_chart("ingredients_validated", PALETTE["accent2"], "Ingrédients", nutri_scores, cook_cats,
                              kcal_min, kcal_max, top_n)


def tags_top_chart(nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None, top_n=15) -> go.Figure:
    return _generic_top_chart("tags", PALETTE["accent3"], "Tags & Catégories", nutri_scores, cook_cats, kcal_min,
                              kcal_max, top_n)

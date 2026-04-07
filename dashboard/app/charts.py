"""
Usines à figures Plotly — toutes les fonctions retournent un go.Figure.
Toutes acceptent des paramètres de filtre optionnels pour le dashboard dynamique.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from .config import PALETTE, NUTRI_COLORS, PLOT_LAYOUT
from .data import con


def _build_where(
    nutri_scores: list[str] | None = None,
    cook_cats: list[str] | None = None,
    kcal_min: float | None = None,
    kcal_max: float | None = None,
    table_prefix: str = "",  # "m." ou ""
) -> tuple[str, list]:
    """Construit la clause WHERE et les paramètres à partir des filtres actifs."""
    clauses = []
    params = []
    p = table_prefix  # ex: "m." pour un JOIN

    if nutri_scores:
        placeholders = ",".join(["?"] * len(nutri_scores))
        clauses.append(f"{p}nutri_score IN ({placeholders})")
        params.extend(nutri_scores)

    if cook_cats:
        placeholders = ",".join(["?"] * len(cook_cats))
        clauses.append(f"{p}cook_time_category IN ({placeholders})")
        params.extend(cook_cats)

    if kcal_min is not None:
        clauses.append(f"{p}energy_kcal >= ?")
        params.append(kcal_min)

    if kcal_max is not None:
        clauses.append(f"{p}energy_kcal <= ?")
        params.append(kcal_max)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def kcal_histogram(
    nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None
) -> go.Figure:
    base_where = "WHERE energy_kcal IS NOT NULL AND energy_kcal < 3500"
    extra_clauses = []
    params = []

    if nutri_scores:
        placeholders = ",".join(["?"] * len(nutri_scores))
        extra_clauses.append(f"nutri_score IN ({placeholders})")
        params.extend(nutri_scores)
    if cook_cats:
        placeholders = ",".join(["?"] * len(cook_cats))
        extra_clauses.append(f"cook_time_category IN ({placeholders})")
        params.extend(cook_cats)
    if kcal_min is not None:
        extra_clauses.append("energy_kcal >= ?")
        params.append(kcal_min)
    if kcal_max is not None:
        extra_clauses.append("energy_kcal <= ?")
        params.append(kcal_max)

    extra = (" AND " + " AND ".join(extra_clauses)) if extra_clauses else ""
    sql = f"SELECT energy_kcal FROM recipes_main {base_where}{extra}"
    df = con.cursor().execute(sql, params).df()

    n = len(df)
    fig = px.histogram(df, x="energy_kcal", nbins=60, color_discrete_sequence=[PALETTE["accent1"]])
    fig.update_traces(marker_line_width=0.5, marker_line_color="white")
    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(
            text=f"Distribution énergie (kcal) — {n:,} recettes".replace(",", "\u202f"),
            x=0.5, font_size=13,
        ),
        xaxis_title="kcal", yaxis_title="recettes",
    )
    fig.update_xaxes(showgrid=False, zeroline=False, tickfont_size=10)
    fig.update_yaxes(showgrid=True, gridcolor=PALETTE["border"], zeroline=False, tickfont_size=10)
    return fig


def nutri_pie(
    nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None
) -> go.Figure:
    join_clauses = []
    params = []
    if cook_cats or kcal_min is not None or kcal_max is not None or nutri_scores:
        # On joint recipes_main pour filtrer
        joins = "JOIN recipes_main m ON n.recipe_id = m.recipe_id"
        if nutri_scores:
            placeholders = ",".join(["?"] * len(nutri_scores))
            join_clauses.append(f"m.nutri_score IN ({placeholders})")
            params.extend(nutri_scores)
        if cook_cats:
            placeholders = ",".join(["?"] * len(cook_cats))
            join_clauses.append(f"m.cook_time_category IN ({placeholders})")
            params.extend(cook_cats)
        if kcal_min is not None:
            join_clauses.append("m.energy_kcal >= ?")
            params.append(kcal_min)
        if kcal_max is not None:
            join_clauses.append("m.energy_kcal <= ?")
            params.append(kcal_max)
        where = ("WHERE " + " AND ".join(join_clauses)) if join_clauses else ""
        query = f"""
            SELECT SUM(n.fat_g) AS "Matières grasses",
                   SUM(n.protein_g) AS "Protéines",
                   SUM(n.salt_g) AS "Sel",
                   SUM(n.saturates_g) AS "Graisses saturées",
                   SUM(n.sugars_g) AS "Sucres"
            FROM recipes_nutrition n
            {joins}
            {where}
        """
    else:
        query = """
            SELECT SUM(fat_g) AS "Matières grasses",
                   SUM(protein_g) AS "Protéines",
                   SUM(salt_g) AS "Sel",
                   SUM(saturates_g) AS "Graisses saturées",
                   SUM(sugars_g) AS "Sucres"
            FROM recipes_nutrition
        """

    df = con.cursor().execute(query, params).df()

    if df.empty or df.iloc[0].isna().all():
        fig = go.Figure()
        fig.update_layout(
            **PLOT_LAYOUT,
            title=dict(text="Données nutritionnelles indisponibles", x=0.5, font_size=13)
        )
        fig.update_xaxes(visible=False)
        fig.update_yaxes(visible=False)
        return fig

    labels = df.columns.tolist()
    values = df.iloc[0].tolist()
    colors = [
        PALETTE["accent1"], PALETTE["accent2"],
        PALETTE["muted"], PALETTE["accent3"], PALETTE["accent4"],
    ]
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.52,
        marker=dict(colors=colors, line=dict(color="white", width=2)),
        textinfo="label+percent", textposition="outside", textfont_size=10,
    ))
    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text="Répartition nutritionnelle totale", x=0.5, font_size=13),
    )
    return fig


def nutri_bar(
    nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None
) -> go.Figure:
    extra_clauses = ["nutri_score IS NOT NULL"]
    params = []

    if cook_cats:
        placeholders = ",".join(["?"] * len(cook_cats))
        extra_clauses.append(f"cook_time_category IN ({placeholders})")
        params.extend(cook_cats)
    if kcal_min is not None:
        extra_clauses.append("energy_kcal >= ?")
        params.append(kcal_min)
    if kcal_max is not None:
        extra_clauses.append("energy_kcal <= ?")
        params.append(kcal_max)

    where = "WHERE " + " AND ".join(extra_clauses)
    sql = f"""
        SELECT nutri_score AS score, COUNT(*) AS count
        FROM recipes_main
        {where}
        GROUP BY nutri_score
        ORDER BY score
    """
    counts = con.cursor().execute(sql, params).df()

    # Colorer différemment les scores sélectionnés
    bar_colors = []
    for s in counts["score"]:
        base_color = NUTRI_COLORS.get(s, PALETTE["muted"])
        if nutri_scores and s not in nutri_scores:
            # Griser les non-sélectionnés
            bar_colors.append(PALETTE["border"])
        else:
            bar_colors.append(base_color)

    fig = go.Figure(go.Bar(
        x=counts["count"],
        y=counts["score"],
        orientation="h",
        marker_color=bar_colors if bar_colors else [NUTRI_COLORS.get(s, PALETTE["muted"]) for s in counts["score"]],
        customdata=counts["score"],
        hovertemplate="<b>Nutri-Score %{y}</b><br>%{x:,} recettes<extra></extra>",
    ))
    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text="Répartition Nutri-Score  <i>(cliquer pour filtrer)</i>", x=0.5, font_size=13),
        xaxis_title="recettes", yaxis_title="",
        clickmode="event+select",
    )
    fig.update_xaxes(showgrid=True, gridcolor=PALETTE["border"])
    fig.update_yaxes(showgrid=False, categoryorder="category ascending")
    return fig


def cook_time_chart(
    nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None
) -> go.Figure:
    extra_clauses = ["cook_time_category IN ('rapide', 'moyen', 'long')"]
    params = []

    if nutri_scores:
        placeholders = ",".join(["?"] * len(nutri_scores))
        extra_clauses.append(f"nutri_score IN ({placeholders})")
        params.extend(nutri_scores)
    if kcal_min is not None:
        extra_clauses.append("energy_kcal >= ?")
        params.append(kcal_min)
    if kcal_max is not None:
        extra_clauses.append("energy_kcal <= ?")
        params.append(kcal_max)

    where = "WHERE " + " AND ".join(extra_clauses)
    sql = f"""
        SELECT cook_time_category, COUNT(*) AS count
        FROM recipes_main
        {where}
        GROUP BY cook_time_category
    """
    df = con.cursor().execute(sql, params).df()
    ct = df.set_index("cook_time_category").reindex(["rapide", "moyen", "long"]).reset_index()

    bar_colors = []
    for cat in ["rapide", "moyen", "long"]:
        base = [PALETTE["accent2"], PALETTE["accent4"], PALETTE["accent3"]][["rapide", "moyen", "long"].index(cat)]
        if cook_cats and cat not in cook_cats:
            bar_colors.append(PALETTE["border"])
        else:
            bar_colors.append(base)

    fig = go.Figure(go.Bar(
        x=ct["cook_time_category"], y=ct["count"],
        marker_color=bar_colors, marker_line_width=0,
        text=ct["count"].apply(lambda x: f"{int(x):,}".replace(",", "\u202f") if pd.notna(x) else "0"),
        textposition="outside", textfont_size=10,
        hovertemplate="<b>%{x}</b><br>%{y:,} recettes<extra></extra>",
        customdata=ct["cook_time_category"],
    ))
    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text="Temps de cuisson  <i>(cliquer pour filtrer)</i>", x=0.5, font_size=13),
        xaxis_title="", yaxis_title="recettes",
        yaxis=dict(showgrid=True, gridcolor=PALETTE["border"]),
        clickmode="event+select",
    )
    return fig


def cook_time_curve(
    nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None
) -> go.Figure:
    extra_clauses = ["cook_minutes IS NOT NULL", "cook_minutes BETWEEN 1 AND 300"]
    params = []

    if nutri_scores:
        placeholders = ",".join(["?"] * len(nutri_scores))
        extra_clauses.append(f"nutri_score IN ({placeholders})")
        params.extend(nutri_scores)
    if cook_cats:
        placeholders = ",".join(["?"] * len(cook_cats))
        extra_clauses.append(f"cook_time_category IN ({placeholders})")
        params.extend(cook_cats)
    if kcal_min is not None:
        extra_clauses.append("energy_kcal >= ?")
        params.append(kcal_min)
    if kcal_max is not None:
        extra_clauses.append("energy_kcal <= ?")
        params.append(kcal_max)

    where = "WHERE " + " AND ".join(extra_clauses)
    sql = f"""
        SELECT FLOOR(cook_minutes / 5) * 5 AS bucket, COUNT(*) AS count
        FROM recipes_main
        {where}
        GROUP BY bucket
        ORDER BY bucket
    """
    df = con.cursor().execute(sql, params).df()

    fig = go.Figure(go.Scatter(
        x=df["bucket"], y=df["count"], mode="lines",
        line=dict(color=PALETTE["accent4"], width=2.5, shape="spline", smoothing=1.2),
        fill="tozeroy", fillcolor="rgba(74, 111, 165, 0.13)",
        hovertemplate="%{x} min — %{y:,} recettes<extra></extra>",
    ))
    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text="Distribution des temps de cuisson (min)", x=0.5, font_size=13),
        xaxis_title="minutes", yaxis_title="recettes",
        xaxis=dict(showgrid=False, zeroline=False, tickfont_size=10),
        yaxis=dict(showgrid=True, gridcolor=PALETTE["border"], zeroline=False, tickfont_size=10),
    )
    return fig


def scatter_saturates_sugars(
    nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None
) -> go.Figure:
    extra_clauses = [
        "n.saturates_g IS NOT NULL", "n.sugars_g IS NOT NULL",
        "m.nutri_score IS NOT NULL",
        "n.saturates_g BETWEEN 0 AND 60",
        "n.sugars_g BETWEEN 0 AND 100",
    ]
    params = []

    if nutri_scores:
        placeholders = ",".join(["?"] * len(nutri_scores))
        extra_clauses.append(f"m.nutri_score IN ({placeholders})")
        params.extend(nutri_scores)
    if cook_cats:
        placeholders = ",".join(["?"] * len(cook_cats))
        extra_clauses.append(f"m.cook_time_category IN ({placeholders})")
        params.extend(cook_cats)
    if kcal_min is not None:
        extra_clauses.append("m.energy_kcal >= ?")
        params.append(kcal_min)
    if kcal_max is not None:
        extra_clauses.append("m.energy_kcal <= ?")
        params.append(kcal_max)

    where = "WHERE " + " AND ".join(extra_clauses)
    sql = f"""
        SELECT n.saturates_g, n.sugars_g, COALESCE(m.nutri_score, '?') AS nutri_score, m.title
        FROM recipes_nutrition n
        JOIN recipes_main m ON n.recipe_id = m.recipe_id
        {where}
        USING SAMPLE 2000 ROWS
    """
    df = con.cursor().execute(sql, params).df()
    df["nutri_score"] = pd.Categorical(
        df["nutri_score"], categories=["A", "B", "C", "D", "E", "?"], ordered=True
    )
    df = df.sort_values("nutri_score")

    fig = go.Figure()
    for score in ["A", "B", "C", "D", "E", "?"]:
        sub = df[df["nutri_score"] == score]
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["saturates_g"], y=sub["sugars_g"], mode="markers",
            name=f"Score {score}",
            text=sub["title"],
            marker=dict(
                color=NUTRI_COLORS.get(score, PALETTE["muted"]),
                size=5, opacity=0.60, line=dict(width=0),
            ),
            hovertemplate=(
                "<b>%{text}</b><br>"
                f"Nutri-Score {score}<br>"
                "Graisses saturées : %{x:.1f} g<br>"
                "Sucres : %{y:.1f} g<extra></extra>"
            ),
        ))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="'Playfair Display', Georgia, serif", color=PALETTE["text"], size=11),
        margin=dict(l=10, r=10, t=50, b=10), showlegend=True,
        title=dict(text="Sucres vs Graisses saturées (axes du Nutri-Score)", x=0.5, font_size=13),
        xaxis_title="graisses saturées (g)",
        yaxis_title="sucres (g)",
        xaxis=dict(showgrid=True, gridcolor=PALETTE["border"], zeroline=False, tickfont_size=10),
        yaxis=dict(showgrid=True, gridcolor=PALETTE["border"], zeroline=False, tickfont_size=10),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
            font=dict(size=10), bgcolor="rgba(0,0,0,0)", borderwidth=0, itemsizing="constant",
        ),
    )
    return fig


def ingredients_top_chart(
    nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None,
    top_n: int = 15,
) -> go.Figure:
    """
    Nouveau graphique dynamique : Top N ingrédients les plus fréquents selon les filtres.
    """
    extra_clauses = []
    params = []

    if nutri_scores:
        placeholders = ",".join(["?"] * len(nutri_scores))
        extra_clauses.append(f"nutri_score IN ({placeholders})")
        params.extend(nutri_scores)
    if cook_cats:
        placeholders = ",".join(["?"] * len(cook_cats))
        extra_clauses.append(f"cook_time_category IN ({placeholders})")
        params.extend(cook_cats)
    if kcal_min is not None:
        extra_clauses.append("energy_kcal >= ?")
        params.append(kcal_min)
    if kcal_max is not None:
        extra_clauses.append("energy_kcal <= ?")
        params.append(kcal_max)

    where = ("WHERE " + " AND ".join(extra_clauses)) if extra_clauses else ""

    # UNNEST du tableau d'ingrédients validés
    sql = f"""
        SELECT ingredient, COUNT(*) AS freq
        FROM (
            SELECT UNNEST(ingredients_validated) AS ingredient
            FROM recipes_main
            {where}
        ) sub
        WHERE ingredient IS NOT NULL AND LENGTH(TRIM(ingredient)) > 1
        GROUP BY ingredient
        ORDER BY freq DESC
        LIMIT ?
    """
    params.append(top_n)

    try:
        df = con.cursor().execute(sql, params).df()
    except Exception:
        df = pd.DataFrame(columns=["ingredient", "freq"])

    if df.empty:
        fig = go.Figure()
        fig.update_layout(**PLOT_LAYOUT, title=dict(text="Aucun ingrédient disponible", x=0.5, font_size=13))
        return fig

    fig = go.Figure(go.Bar(
        x=df["freq"],
        y=df["ingredient"],
        orientation="h",
        marker_color=PALETTE["accent2"],
        marker_line_width=0,
        hovertemplate="<b>%{y}</b><br>%{x:,} recettes<extra></extra>",
    ))
    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text=f"Top {top_n} ingrédients", x=0.5, font_size=13),
        xaxis_title="recettes", yaxis_title="",
        yaxis=dict(autorange="reversed", tickfont_size=10),
        xaxis=dict(showgrid=True, gridcolor=PALETTE["border"]),
        clickmode="event+select"
    )
    return fig

def tags_top_chart(
    nutri_scores=None, cook_cats=None, kcal_min=None, kcal_max=None,
    top_n: int = 15,
) -> go.Figure:
    """
    Nouveau graphique dynamique : Top N des tags / catégories.
    """
    extra_clauses = []
    params = []

    if nutri_scores:
        placeholders = ",".join(["?"] * len(nutri_scores))
        extra_clauses.append(f"nutri_score IN ({placeholders})")
        params.extend(nutri_scores)
    if cook_cats:
        placeholders = ",".join(["?"] * len(cook_cats))
        extra_clauses.append(f"cook_time_category IN ({placeholders})")
        params.extend(cook_cats)
    if kcal_min is not None:
        extra_clauses.append("energy_kcal >= ?")
        params.append(kcal_min)
    if kcal_max is not None:
        extra_clauses.append("energy_kcal <= ?")
        params.append(kcal_max)

    where = ("WHERE " + " AND ".join(extra_clauses)) if extra_clauses else ""

    # UNNEST du tableau des tags
    sql = f"""
        SELECT tag, COUNT(*) AS freq
        FROM (
            SELECT UNNEST(tags) AS tag
            FROM recipes_main
            {where}
        ) sub
        WHERE tag IS NOT NULL AND LENGTH(TRIM(tag)) > 1
        GROUP BY tag
        ORDER BY freq DESC
        LIMIT ?
    """
    params.append(top_n)

    try:
        df = con.cursor().execute(sql, params).df()
    except Exception:
        df = pd.DataFrame(columns=["tag", "freq"])

    if df.empty:
        fig = go.Figure()
        fig.update_layout(**PLOT_LAYOUT, title=dict(text="Aucun tag disponible", x=0.5, font_size=13))
        return fig

    # On utilise "accent3" pour le différencier visuellement des ingrédients
    fig = go.Figure(go.Bar(
        x=df["freq"],
        y=df["tag"],
        orientation="h",
        marker_color=PALETTE["accent3"],
        marker_line_width=0,
        hovertemplate="<b>%{y}</b><br>%{x:,} recettes<extra></extra>",
    ))
    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text=f"Top {top_n} Tags & Catégories", x=0.5, font_size=13),
        xaxis_title="recettes", yaxis_title="",
        yaxis=dict(autorange="reversed", tickfont_size=10),
        xaxis=dict(showgrid=True, gridcolor=PALETTE["border"]),
    )
    return fig
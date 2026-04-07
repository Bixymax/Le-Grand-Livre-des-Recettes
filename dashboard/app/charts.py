"""
Usines à figures Plotly — toutes les fonctions retournent un go.Figure.
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from .config import PALETTE, NUTRI_COLORS, PLOT_LAYOUT
from .data import con


def kcal_histogram() -> go.Figure:
    # OPT P7 : lit uniquement recipes_main, pas la vue avec JOIN
    df = con.cursor().execute(
    "SELECT energy_kcal FROM recipes_main WHERE energy_kcal IS NOT NULL AND energy_kcal < 3500"
    ).df()
    fig = px.histogram(df, x="energy_kcal", nbins=60, color_discrete_sequence=[PALETTE["accent1"]])
    fig.update_traces(marker_line_width=0.5, marker_line_color="white")
    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text="Distribution énergie (kcal)", x=0.5, font_size=13),
        xaxis_title="kcal", yaxis_title="recettes",
    )
    fig.update_xaxes(showgrid=False, zeroline=False, tickfont_size=10)
    fig.update_yaxes(showgrid=True, gridcolor=PALETTE["border"], zeroline=False, tickfont_size=10)
    return fig


def nutri_pie() -> go.Figure:
    query = """
            SELECT SUM(fat_g)       AS "Matières grasses",
                   SUM(protein_g)   AS "Protéines",
                   SUM(salt_g)      AS "Sel",
                   SUM(saturates_g) AS "Graisses saturées",
                   SUM(sugars_g)    AS "Sucres"
            FROM recipes_nutrition \
            """
    df = con.cursor().execute(query).df()

    # --- SAFETY CHECK: IF DATAFRAME IS EMPTY OR ALL NULL ---
    if df.empty or df.iloc[0].isna().all():
        fig = go.Figure()
        fig.update_layout(
            **PLOT_LAYOUT,
            title=dict(text="Données nutritionnelles indisponibles", x=0.5, font_size=13)
        )
        # Removes axes so it looks clean
        fig.update_xaxes(visible=False)
        fig.update_yaxes(visible=False)
        return fig
    # --------------------------------------------------------

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


def nutri_bar() -> go.Figure:
    # OPT P7 : lit uniquement recipes_main
    counts = con.execute("""
        SELECT nutri_score AS score, COUNT(*) AS count
        FROM recipes_main WHERE nutri_score IS NOT NULL
        GROUP BY nutri_score ORDER BY score
    """).df()
    fig = px.bar(
        counts, x="count", y="score", orientation="h",
        color="score", color_discrete_map=NUTRI_COLORS,
    )
    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text="Répartition Nutri-Score", x=0.5, font_size=13),
        xaxis_title="recettes", yaxis_title="",
    )
    fig.update_xaxes(showgrid=True, gridcolor=PALETTE["border"])
    fig.update_yaxes(showgrid=False, categoryorder="category ascending")
    return fig


def cook_time_chart() -> go.Figure:
    # OPT P7 : lit uniquement recipes_main
    df = con.execute("""
        SELECT cook_time_category, COUNT(*) AS count
        FROM recipes_main WHERE cook_time_category IN ('rapide', 'moyen', 'long')
        GROUP BY cook_time_category
    """).df()
    ct = df.set_index("cook_time_category").reindex(["rapide", "moyen", "long"]).reset_index()
    colors = [PALETTE["accent2"], PALETTE["accent4"], PALETTE["accent3"]]
    fig = go.Figure(go.Bar(
        x=ct["cook_time_category"], y=ct["count"], marker_color=colors, marker_line_width=0,
        text=ct["count"].apply(lambda x: f"{x:,}" if pd.notna(x) else "0"),
        textposition="outside", textfont_size=10,
    ))
    fig.update_layout(
        **PLOT_LAYOUT, title=dict(text="Temps de cuisson", x=0.5, font_size=13),
        xaxis_title="", yaxis_title="recettes",
        yaxis=dict(showgrid=True, gridcolor=PALETTE["border"]),
    )
    return fig


def cook_time_curve() -> go.Figure:
    # OPT P7 : lit uniquement recipes_main
    df = con.execute("""
        SELECT FLOOR(cook_minutes / 5) * 5 AS bucket, COUNT(*) AS count
        FROM recipes_main WHERE cook_minutes IS NOT NULL AND cook_minutes BETWEEN 1 AND 300
        GROUP BY bucket ORDER BY bucket
    """).df()
    fig = go.Figure(go.Scatter(
        x=df["bucket"], y=df["count"], mode="lines",
        line=dict(color=PALETTE["accent4"], width=2.5, shape="spline", smoothing=1.2),
        fill="tozeroy", fillcolor="rgba(74, 111, 165, 0.13)",
    ))
    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text="Distribution des temps de cuisson (min)", x=0.5, font_size=13),
        xaxis_title="minutes", yaxis_title="recettes",
        xaxis=dict(showgrid=False, zeroline=False, tickfont_size=10),
        yaxis=dict(showgrid=True, gridcolor=PALETTE["border"], zeroline=False, tickfont_size=10),
    )
    return fig


def scatter_saturates_sugars() -> go.Figure:
    """
    Graisses saturées (x) vs Sucres (y), coloré par Nutri-Score.
    OPT P7 : joint recipes_nutrition_detail ← recipes_main plutôt que la vue recipes.
    """
    df = con.execute("""
        SELECT n.saturates_g, n.sugars_g, COALESCE(m.nutri_score, '?') AS nutri_score
        FROM recipes_nutrition n
        JOIN recipes_main m ON n.recipe_id = m.recipe_id
        WHERE n.saturates_g IS NOT NULL
          AND n.sugars_g    IS NOT NULL
          AND m.nutri_score IS NOT NULL
          AND n.saturates_g BETWEEN 0 AND 60
          AND n.sugars_g    BETWEEN 0 AND 100
        USING SAMPLE 2000 ROWS
    """).df()
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
            marker=dict(
                color=NUTRI_COLORS.get(score, PALETTE["muted"]),
                size=5, opacity=0.60, line=dict(width=0),
            ),
            hovertemplate=(
                f"<b>Nutri-Score {score}</b><br>"
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

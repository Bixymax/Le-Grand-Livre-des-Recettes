"""
Callbacks Dash — version optimisée sans fallbacks.
"""

import ast
import json
import random

import dash
import numpy as np
import pandas as pd
from dash import Input, Output, State, html

from .config import PALETTE, NUTRI_COLORS
from .data import con, RECIPE_COLS, RECIPE_IDS_WITH_IMAGE
from .charts import (
    kcal_histogram, nutri_pie, nutri_bar,
    cook_time_chart, cook_time_curve, scatter_saturates_sugars,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_instructions(raw_text: str) -> list[str]:
    if not isinstance(raw_text, str) or not raw_text:
        return ["1. Instructions non disponibles."]
    raw_steps = raw_text.split("|")
    cleaned = [s.strip() for s in raw_steps if s.strip()]
    return [f"{i + 1}. {step}" for i, step in enumerate(cleaned)]


def _build_recipe_outputs(row):
    img_url = row.get("image_url") or ""
    if not img_url:
        image_urls = row.get("image_urls")
        if image_urls is not None:
            if isinstance(image_urls, np.ndarray):
                image_urls = image_urls.tolist()
            if isinstance(image_urls, list):
                img_url = next((u for u in image_urls if u), "")

    if img_url:
        img_elem = html.Img(src=img_url, style={
            "display": "block", "width": "100%", "height": "100%", "objectFit": "cover",
        })
    else:
        img_elem = html.Div("📷 Pas d'image", style={
            "height": "100%", "width": "100%",
            "display": "flex", "alignItems": "center", "justifyContent": "center",
            "backgroundColor": PALETTE["bg"], "color": PALETTE["muted"], "fontSize": "0.9rem",
        })

    title_str = row.get("title", "—")

    instructions_raw = str(row.get("instructions_text") or "")
    if "|" in instructions_raw:
        steps = _format_instructions(instructions_raw)
    else:
        steps = [s.strip() for s in instructions_raw.split("\n") if s.strip()]
        if not steps:
            steps = ["1. Instructions non disponibles."]

    try:
        cook_min = 0 if pd.isna(row.get("cook_minutes")) else int(row.get("cook_minutes"))
    except (ValueError, TypeError):
        cook_min = 0

    n_score = row.get("nutri_score")
    n_score_display = n_score if pd.notna(n_score) and n_score else "?"
    energy = row.get("energy_kcal")

    def get_macro(key):
        val = row.get(key)
        return str(val) if pd.notna(val) else "-"

    badge_color = NUTRI_COLORS.get(n_score_display, PALETTE["muted"])
    nutri_badge = html.Span(
        f"Nutri-Score {n_score_display}",
        style={
            "backgroundColor": badge_color, "color": "white",
            "padding": "2px 8px", "borderRadius": "12px",
            "fontSize": "0.7rem", "fontWeight": "bold", "marginRight": "8px",
        },
    )
    kcal_text = html.Span(
        f"{int(energy)} kcal" if pd.notna(energy) else "Kcal inconnues",
        style={"fontSize": "0.85rem", "fontWeight": "600", "color": PALETTE["text"]},
    )
    short_text_div = html.Div([
        html.Div(
            f"{len(steps)} étape(s) • {cook_min} min",
            style={"marginBottom": "16px", "color": PALETTE["text"], "fontWeight": "500"},
        ),
        html.Div([nutri_badge, kcal_text],
                 style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}),
        html.Div(
            f"P: {get_macro('protein_g')}g | L: {get_macro('fat_g')}g | G: {get_macro('sugars_g')}g",
            style={"fontSize": "0.75rem", "color": PALETTE["muted"]},
        ),
    ])

    content = [html.P(s, style={"margin": "0 0 6px 0", "breakInside": "avoid"}) for s in steps]

    ings_raw = row.get("ingredients_validated")
    if isinstance(ings_raw, np.ndarray):
        ings_raw = ings_raw.tolist()
    if ings_raw is None or (isinstance(ings_raw, float) and pd.isna(ings_raw)):
        ings_raw = []
    elif isinstance(ings_raw, str):
        try:
            ings_raw = ast.literal_eval(ings_raw)
        except (ValueError, SyntaxError):
            ings_raw = [i.strip() for i in ings_raw.split(",") if i.strip()]

    ings_items = (
        [html.Li(f"– {ing}", style={"paddingLeft": "4px"}) for ing in ings_raw[:14]]
        if ings_raw
        else [html.Li("– Non disponible", style={"color": PALETTE["muted"]})]
    )

    return img_elem, title_str, 0, short_text_div, content, ings_items


def _fetch_recipe_by_id(recipe_id: str):
    return con.cursor().execute(
        f"""
        SELECT {RECIPE_COLS}
        FROM recipes_main m
        LEFT JOIN recipes_nutrition n ON m.recipe_id = n.recipe_id
        WHERE m.recipe_id = ?
        LIMIT 1
        """,
        [recipe_id],
    ).df()


# ---------------------------------------------------------------------------
# Enregistrement des callbacks
# ---------------------------------------------------------------------------

def register_callbacks(app: dash.Dash):

    @app.callback(
        Output("graph-kcal-hist",    "figure"),
        Output("graph-nutri-pie",    "figure"),
        Output("graph-nutri-bar",    "figure"),
        Output("graph-cook-times",   "figure"),
        Output("graph-cook-curve",   "figure"),
        Output("graph-scatter-kcal", "figure"),
        Input("init-interval", "n_intervals"),
    )
    def load_all_charts(_):
        return (
            kcal_histogram(),
            nutri_pie(),
            nutri_bar(),
            cook_time_chart(),
            cook_time_curve(),
            scatter_saturates_sugars(),
        )

    @app.callback(
        Output("search-results", "children"),
        Input("btn-search", "n_clicks"),
        Input("search-input", "n_submit"),
        State("search-input", "value"),
        prevent_initial_call=True,
    )
    def search_recipes(n_clicks, n_submit, query):
        if not query or not query.strip():
            return ""

        try:
            df_res = con.cursor().execute(
                """
                SELECT recipe_id, title, nutri_score, cook_time_category,
                       fts_main_recipes_main.match_bm25(recipe_id, ?) AS score
                FROM recipes_main
                WHERE fts_main_recipes_main.match_bm25(recipe_id, ?) IS NOT NULL
                ORDER BY score DESC
                LIMIT 8
                """,
                [query.strip(), query.strip()],
            ).df()

            if df_res.empty:
                return html.Span(
                    "Aucun résultat trouvé.",
                    style={"color": PALETTE["muted"], "fontStyle": "italic"},
                )

            items = []
            for _, row in df_res.iterrows():
                score_label = row.get("nutri_score") or "?"
                badge_color = NUTRI_COLORS.get(score_label, PALETTE["muted"])
                recipe_id = str(row.get("recipe_id", ""))
                items.append(html.Div(
                    id={"type": "search-result-item", "index": recipe_id},
                    style={
                        "display": "flex", "alignItems": "center", "gap": "8px",
                        "marginBottom": "4px", "cursor": "pointer",
                        "padding": "4px 6px", "borderRadius": "6px",
                        "transition": "background 0.15s",
                    },
                    className="search-result-row",
                    children=[
                        html.Span(score_label, style={
                            "backgroundColor": badge_color, "color": "white",
                            "padding": "1px 6px", "borderRadius": "8px",
                            "fontSize": "0.68rem", "fontWeight": "bold", "flexShrink": "0",
                        }),
                        html.Span(row.get("title", "—"),
                                  style={"color": PALETTE["text"], "flex": "1"}),
                        html.Span(
                            f"· {row.get('cook_time_category', '')}",
                            style={"color": PALETTE["muted"], "fontSize": "0.72rem"},
                        ),
                        html.Span("›", style={
                            "color": PALETTE["muted"], "fontSize": "0.9rem", "flexShrink": "0",
                        }),
                    ],
                ))
            return items

        except Exception as e:
            return html.Span(
                f"Erreur recherche : {e}",
                style={"color": PALETTE["accent3"], "fontStyle": "italic"},
            )


    @app.callback(
        Output("store-selected-recipe-id", "data"),
        Input({"type": "search-result-item", "index": dash.ALL}, "n_clicks"),
        State({"type": "search-result-item", "index": dash.ALL}, "id"),
        prevent_initial_call=True,
    )
    def store_clicked_recipe(n_clicks_list, ids):
        if not n_clicks_list or not any(n_clicks_list):
            return dash.no_update
        ctx = dash.callback_context
        if not ctx.triggered:
            return dash.no_update
        triggered_id = ctx.triggered[0]["prop_id"]
        id_dict = json.loads(triggered_id.replace(".n_clicks", ""))
        return id_dict.get("index")


    @app.callback(
        Output("recipe-image-container",    "children"),
        Output("recipe-title",              "children"),
        Output("store-recipe-idx",          "data"),
        Output("recipe-instructions-short", "children"),
        Output("recipe-instructions-content","children"),
        Output("ingredients-list",          "children"),
        Input("btn-random-recipe",          "n_clicks"),
        Input("init-interval",              "n_intervals"),
        Input("store-selected-recipe-id",   "data"),
        prevent_initial_call=False,
    )
    def update_recipe_panel(n_clicks, n_intervals, selected_recipe_id):
        ctx = dash.callback_context
        triggered = ctx.triggered[0]["prop_id"] if ctx.triggered else ""

        if "store-selected-recipe-id" in triggered and selected_recipe_id:
            df = _fetch_recipe_by_id(str(selected_recipe_id))
            if not df.empty:
                return _build_recipe_outputs(df.iloc[0])

        rid = random.choice(RECIPE_IDS_WITH_IMAGE)
        df_recipe = _fetch_recipe_by_id(rid)

        if df_recipe.empty:
            return html.Div("Pas d'image"), "Pas de recette", 0, "", "", ""

        return _build_recipe_outputs(df_recipe.iloc[0])
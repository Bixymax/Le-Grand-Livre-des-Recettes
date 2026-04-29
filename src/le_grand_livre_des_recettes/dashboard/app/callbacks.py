"""
Callbacks Dash pour l'interactivité du Grand Livre des Recettes.
Gère les filtres globaux, le cross-filtering, la recherche FTS et le chargement asynchrone.
"""

import ast
import json
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

import dash
import numpy as np
import pandas as pd
from dash import Input, Output, State, html, dcc

from .charts import (
    kcal_histogram, nutri_pie, nutri_bar,
    cook_time_chart, cook_time_curve, scatter_saturates_sugars,
    ingredients_top_chart, tags_top_chart
)
from .config import PALETTE, NUTRI_COLORS
from .data import con, RECIPE_COLS


# Helpers : Traitement des données
def _find_valid_image_url(image_url: str, image_urls: list | np.ndarray) -> str:
    """Retourne la première URL d'image valide (HTTP 200)."""
    candidates = [image_url] if image_url else []

    if image_urls is not None:
        urls_list = image_urls.tolist() if isinstance(image_urls, np.ndarray) else image_urls
        candidates.extend([u for u in urls_list if u and u not in candidates])

    for url in candidates:
        try:
            # Timeout court pour ne pas figer le layout en cas d'URL morte
            req = urlopen(url, timeout=2)
            content_type = req.headers.get("Content-Type", "")
            if req.status == 200 and ("image" in content_type or not content_type):
                return url
        except (HTTPError, URLError, Exception):
            continue

    return ""


def _format_instructions(raw_text: str) -> list[str]:
    """Nettoie et formate le texte brut des instructions."""
    if not isinstance(raw_text, str) or not raw_text:
        return ["1. Instructions non disponibles."]

    # Séparation selon le délimiteur existant ou par saut de ligne
    raw_steps = raw_text.split("|") if "|" in raw_text else raw_text.split("\n")
    cleaned = [s.strip() for s in raw_steps if s.strip()]

    if not cleaned:
        return ["1. Instructions non disponibles."]

    return [f"{i + 1}. {step}" for i, step in enumerate(cleaned)]


def _build_recipe_text_outputs(row: pd.Series) -> tuple:
    """Génère les composants HTML à partir d'une ligne du DataFrame recette."""
    title_str = row.get("title", "—")
    steps = _format_instructions(str(row.get("instructions_text") or ""))

    # Gestion sécurisée du temps de cuisson
    try:
        cook_min = 0 if pd.isna(row.get("cook_minutes")) else int(row.get("cook_minutes"))
    except (ValueError, TypeError):
        cook_min = 0

    n_score = row.get("nutri_score")
    n_score_display = n_score if pd.notna(n_score) and n_score else "?"
    energy = row.get("mit_energy_kcal")

    def get_macro(key: str) -> str:
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

    # Parsing robuste des ingrédients
    ings_raw = row.get("ingredients_validated")
    if isinstance(ings_raw, np.ndarray):
        ings_raw = ings_raw.tolist()

    if not ings_raw or pd.isna(ings_raw).all() if isinstance(ings_raw, float) else False:
        ings_raw = []
    elif isinstance(ings_raw, str):
        try:
            ings_raw = ast.literal_eval(ings_raw)
        except (ValueError, SyntaxError):
            ings_raw = [i.strip() for i in ings_raw.split(",") if i.strip()]

    ings_items = (
        [html.Li(f"– {ing}", style={"paddingLeft": "4px"}) for ing in ings_raw[:14]]
        if ings_raw else [html.Li("– Non disponible", style={"color": PALETTE["muted"]})]
    )

    return title_str, 0, short_text_div, content, ings_items


def _placeholder_image() -> html.Div:
    """Retourne un composant d'attente pendant la résolution de l'URL de l'image."""
    return html.Div(
        html.Div("⏳ Chargement de l'image…", style={
            "height": "100%", "width": "100%",
            "display": "flex", "alignItems": "center", "justifyContent": "center",
            "backgroundColor": PALETTE["bg"], "color": PALETTE["muted"], "fontSize": "0.9rem",
        }),
        id="recipe-image-inner",
        style={"width": "100%", "height": "100%"},
    )


def _fetch_recipe_by_id(recipe_id: str) -> pd.DataFrame:
    """Récupère toutes les données nécessaires pour l'affichage d'une recette."""
    return con.execute(f"""
        SELECT {RECIPE_COLS}
        FROM recipes.recipes_main m
        LEFT JOIN recipes.recipes_nutrition_detail n ON m.recipe_id = n.recipe_id
        WHERE m.recipe_id = ?
        LIMIT 1
    """, [recipe_id]).df()


def _extract_recipe_payload(df: pd.DataFrame) -> tuple:
    """Génère la payload complète de mise à jour du layout pour une recette donnée."""
    row = df.iloc[0]
    title, idx, short, content, ings = _build_recipe_text_outputs(row)

    urls = row.get("image_urls")
    urls_list = urls.tolist() if isinstance(urls, np.ndarray) else (urls or [])

    urls_payload = {
        "image_url": row.get("image_url") or "",
        "image_urls": urls_list,
    }
    return _placeholder_image(), title, idx, short, content, ings, urls_payload


def _unpack_filters(filters: dict) -> tuple:
    """Extrait et standardise les filtres du store pour les requêtes SQL."""
    if not filters:
        return None, None, None, None

    nutri = filters.get("nutri_scores") or None
    cook = filters.get("cook_cats") or None
    kcal_min = filters.get("kcal_min")
    kcal_max = filters.get("kcal_max")

    kcal_min = None if kcal_min == 0 else kcal_min
    kcal_max = None if kcal_max == 3500 else kcal_max

    return nutri, cook, kcal_min, kcal_max


# Enregistrement des callbacks
def register_callbacks(app: dash.Dash):

    # Gestion du panneau de filtres (Store)
    @app.callback(
        Output("store-filters", "data"),
        Output("filter-nutri", "value"),
        Output("filter-cook", "value"),
        Output("filter-kcal", "value"),
        Input("filter-nutri", "value"),
        Input("filter-cook", "value"),
        Input("filter-kcal", "value"),
        Input("graph-nutri-bar", "clickData"),
        Input("graph-cook-times", "clickData"),
        Input("graph-kcal-hist", "clickData"),
        Input("btn-reset-filters", "n_clicks"),
        State("store-filters", "data"),
        prevent_initial_call=False,
    )
    def update_filter_store(nutri_val, cook_val, kcal_val, nutri_click, cook_click,
                            kcal_hist_click, reset_clicks, current_filters):
        triggered_id = dash.ctx.triggered_id or ""

        if triggered_id == "btn-reset-filters":
            return {"nutri_scores": [], "cook_cats": [], "kcal_min": 0, "kcal_max": 3500}, [], [], [0, 3500]

        # Cross-filtering : clic sur la répartition Nutri-Score
        if triggered_id == "graph-nutri-bar" and nutri_click:
            clicked_score = nutri_click["points"][0]["y"]
            current_nutri = list(nutri_val or [])
            if clicked_score in current_nutri:
                current_nutri.remove(clicked_score)
            else:
                current_nutri.append(clicked_score)

            new_filters = {**(current_filters or {}), "nutri_scores": current_nutri}
            return new_filters, current_nutri, cook_val, kcal_val

        # Cross-filtering : clic sur les temps de cuisson
        if triggered_id == "graph-cook-times" and cook_click:
            clicked_cat = cook_click["points"][0]["x"]
            current_cook = list(cook_val or [])
            if clicked_cat in current_cook:
                current_cook.remove(clicked_cat)
            else:
                current_cook.append(clicked_cat)

            new_filters = {**(current_filters or {}), "cook_cats": current_cook}
            return new_filters, nutri_val, current_cook, kcal_val

        # Cross-filtering : clic sur une barre de l'histogramme kcal
        if triggered_id == "graph-kcal-hist" and kcal_hist_click:
            point = kcal_hist_click["points"][0]

            # Plotly expose x0/x1 pour les histogrammes, mais parfois seulement x
            x0 = point.get("x0")
            x1 = point.get("x1")

            if x0 is None or x1 is None:
                # Fallback : x est le centre de la barre — on reconstitue la plage
                # en snappant au step=50 du slider
                x_center = point.get("x", 0)
                x0 = (int(x_center) // 50) * 50
                x1 = x0 + 50

            bin_min = max(0,    int(round(min(x0, x1) / 50) * 50))
            bin_max = min(3500, int(round(max(x0, x1) / 50) * 50))

            # Toggle : re-cliquer la même barre réinitialise
            cur_min = (current_filters or {}).get("kcal_min", 0)
            cur_max = (current_filters or {}).get("kcal_max", 3500)

            if cur_min == bin_min and cur_max == bin_max:
                new_min, new_max = 0, 3500
            else:
                new_min, new_max = bin_min, bin_max

            new_filters = {**(current_filters or {}), "kcal_min": new_min, "kcal_max": new_max}
            return new_filters, nutri_val, cook_val, [new_min, new_max]

        # Modification manuelle depuis les composants UI
        new_filters = {
            "nutri_scores": nutri_val or [],
            "cook_cats": cook_val or [],
            "kcal_min": kcal_val[0] if kcal_val else 0,
            "kcal_max": kcal_val[1] if kcal_val else 3500,
        }
        return new_filters, nutri_val, cook_val, kcal_val

    # Mise à jour du label dynamique du slider
    @app.callback(
        Output("filter-kcal-label", "children"),
        Input("filter-kcal", "value"),
    )
    def update_kcal_label(kcal_val):
        if not kcal_val or (kcal_val[0] == 0 and kcal_val[1] == 3500):
            return "Énergie : toutes"
        return f"Énergie : {kcal_val[0]} – {kcal_val[1]} kcal"

    # Affichage des badges de filtres actifs
    @app.callback(
        Output("active-filters-display", "children"),
        Input("store-filters", "data"),
    )
    def display_active_filters(filters):
        if not filters:
            return []

        badges = []

        # Badges Nutri-Score
        for score in filters.get("nutri_scores", []):
            color = NUTRI_COLORS.get(score, PALETTE["muted"])
            badges.append(html.Span(f"Nutri-Score {score}", style={
                "backgroundColor": color, "color": "white", "padding": "2px 8px",
                "borderRadius": "12px", "fontSize": "0.7rem", "fontWeight": "600", "marginRight": "4px",
            }))

        # Badges Cuisson
        cook_colors = {"rapide": PALETTE["accent2"], "moyen": PALETTE["accent4"], "long": PALETTE["accent3"]}
        cook_labels = {"rapide": "⚡ Rapide", "moyen": "⏱ Moyen", "long": "🕐 Long"}
        for cat in filters.get("cook_cats", []):
            badges.append(html.Span(cook_labels.get(cat, cat), style={
                "backgroundColor": cook_colors.get(cat, PALETTE["muted"]), "color": "white",
                "padding": "2px 8px", "borderRadius": "12px", "fontSize": "0.7rem",
                "fontWeight": "600", "marginRight": "4px",
            }))

        # Badge Kcal
        k_min, k_max = filters.get("kcal_min", 0), filters.get("kcal_max", 3500)
        if k_min != 0 or k_max != 3500:
            badges.append(html.Span(f"⚡ {k_min}–{k_max} kcal", style={
                "backgroundColor": PALETTE["accent1"], "color": "white", "padding": "2px 8px",
                "borderRadius": "12px", "fontSize": "0.7rem", "fontWeight": "600", "marginRight": "4px",
            }))

        if not badges:
            return [html.Span("Aucun filtre actif", style={"fontSize": "0.7rem", "color": PALETTE["muted"], "fontStyle": "italic"})]

        return badges

    # Mise à jour de tous les graphiques
    @app.callback(
        Output("graph-kcal-hist", "figure"),
        Output("graph-nutri-pie", "figure"),
        Output("graph-nutri-bar", "figure"),
        Output("graph-cook-times", "figure"),
        Output("graph-cook-curve", "figure"),
        Output("graph-scatter-kcal", "figure"),
        Output("graph-ingredients-top", "figure"),
        Output("graph-tags-top", "figure"),
        Input("init-interval", "n_intervals"),
        Input("store-filters", "data"),
    )
    def load_all_charts(_, filters):
        nutri, cook, kmin, kmax = _unpack_filters(filters)
        return (
            kcal_histogram(nutri_scores=nutri, cook_cats=cook, kcal_min=kmin, kcal_max=kmax),
            nutri_pie(nutri_scores=nutri, cook_cats=cook, kcal_min=kmin, kcal_max=kmax),
            nutri_bar(nutri_scores=nutri, cook_cats=cook, kcal_min=kmin, kcal_max=kmax),
            cook_time_chart(nutri_scores=nutri, cook_cats=cook, kcal_min=kmin, kcal_max=kmax),
            cook_time_curve(nutri_scores=nutri, cook_cats=cook, kcal_min=kmin, kcal_max=kmax),
            scatter_saturates_sugars(nutri_scores=nutri, cook_cats=cook, kcal_min=kmin, kcal_max=kmax),
            ingredients_top_chart(nutri_scores=nutri, cook_cats=cook, kcal_min=kmin, kcal_max=kmax),
            tags_top_chart(nutri_scores=nutri, cook_cats=cook, kcal_min=kmin, kcal_max=kmax),
        )

    # Moteur de recherche FTS
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
            # Recherche Full-Text via BM25 (très performant sur DuckDB)
            df_res = con.execute("""
                SELECT recipe_id, title, nutri_score, cook_time_category,
                       fts_main_recipes_main.match_bm25(recipe_id, ?) AS score
                FROM recipes.recipes_main
                WHERE fts_main_recipes_main.match_bm25(recipe_id, ?) IS NOT NULL
                ORDER BY score DESC LIMIT 8
            """, [query.strip(), query.strip()]).df()

            if df_res.empty:
                return html.Span("Aucun résultat trouvé.", style={"color": PALETTE["muted"], "fontStyle": "italic"})

            items = []
            for _, row in df_res.iterrows():
                score_label = row.get("nutri_score") or "?"
                badge_color = NUTRI_COLORS.get(score_label, PALETTE["muted"])

                items.append(html.Div(
                    id={"type": "search-result-item", "index": str(row.get("recipe_id", ""))},
                    className="search-result-row",
                    style={
                        "display": "flex", "alignItems": "center", "gap": "8px",
                        "marginBottom": "4px", "cursor": "pointer", "padding": "4px 6px",
                        "borderRadius": "6px", "transition": "background 0.15s",
                    },
                    children=[
                        html.Span(score_label, style={
                            "backgroundColor": badge_color, "color": "white", "padding": "1px 6px",
                            "borderRadius": "8px", "fontSize": "0.68rem", "fontWeight": "bold", "flexShrink": "0",
                        }),
                        html.Span(row.get("title", "—"), style={"color": PALETTE["text"], "flex": "1"}),
                        html.Span(f"· {row.get('cook_time_category', '')}", style={"color": PALETTE["muted"], "fontSize": "0.72rem"}),
                        html.Span("›", style={"color": PALETTE["muted"], "fontSize": "0.9rem", "flexShrink": "0"}),
                    ],
                ))
            return items

        except Exception as e:
            return html.Span(f"Erreur recherche : {e}", style={"color": PALETTE["accent3"], "fontStyle": "italic"})

    # Sauvegarde de la recette sélectionnée
    @app.callback(
        Output("store-selected-recipe-id", "data"),
        Input({"type": "search-result-item", "index": dash.ALL}, "n_clicks"),
        State({"type": "search-result-item", "index": dash.ALL}, "id"),
        prevent_initial_call=True,
    )
    def store_clicked_recipe(n_clicks_list, ids):
        if not n_clicks_list or not any(n_clicks_list):
            return dash.no_update

        ctx_cb = dash.callback_context
        if not ctx_cb.triggered:
            return dash.no_update

        triggered_id = ctx_cb.triggered[0]["prop_id"]
        id_dict = json.loads(triggered_id.replace(".n_clicks", ""))
        return id_dict.get("index")

    # Affichage principal (Texte immédiat + payload Image)
    @app.callback(
        Output("recipe-image-container", "children"),
        Output("recipe-title", "children"),
        Output("store-recipe-idx", "data"),
        Output("recipe-instructions-short", "children"),
        Output("recipe-instructions-content", "children"),
        Output("ingredients-list", "children"),
        Output("store-recipe-image-urls", "data"),
        Input("btn-random-recipe", "n_clicks"),
        Input("init-interval", "n_intervals"),
        Input("store-selected-recipe-id", "data"),
        prevent_initial_call=False,
    )
    def update_recipe_panel(n_clicks, n_intervals, selected_recipe_id):
        triggered_id = dash.ctx.triggered_id or ""

        # Si l'utilisateur a cliqué sur un résultat de recherche
        if triggered_id == "store-selected-recipe-id" and selected_recipe_id:
            df = _fetch_recipe_by_id(str(selected_recipe_id))
            if not df.empty:
                return _extract_recipe_payload(df)

        # Sinon (initialisation ou bouton aléatoire), on prend une recette au hasard avec image
        random_id_df = con.execute("SELECT recipe_id FROM recipes.recipes_main WHERE has_image = true USING SAMPLE 1").df()

        if random_id_df.empty:
            return html.Div("Pas d'image"), "Pas de recette", 0, "", "", "", {}

        df_recipe = _fetch_recipe_by_id(random_id_df.iloc[0]["recipe_id"])
        return _extract_recipe_payload(df_recipe)

    # Résolution asynchrone de l'image
    @app.callback(
        Output("recipe-image-container", "children", allow_duplicate=True),
        Input("store-recipe-image-urls", "data"),
        prevent_initial_call=True,
    )
    def resolve_recipe_image(urls_payload):
        if not urls_payload:
            return dash.no_update

        img_url = _find_valid_image_url(
            urls_payload.get("image_url", ""),
            urls_payload.get("image_urls", []),
        )

        if img_url:
            return html.Div(
                html.Img(src=img_url, style={"display": "block", "width": "100%", "height": "100%", "objectFit": "cover"}),
                id="recipe-image-inner",
                style={"width": "100%", "height": "100%"},
            )

        return html.Div(
            html.Div("📷 Pas d'image disponible", style={
                "height": "100%", "width": "100%", "display": "flex",
                "alignItems": "center", "justifyContent": "center",
                "backgroundColor": PALETTE["bg"], "color": PALETTE["muted"], "fontSize": "0.9rem",
            }),
            id="recipe-image-inner",
            style={"width": "100%", "height": "100%"},
        )
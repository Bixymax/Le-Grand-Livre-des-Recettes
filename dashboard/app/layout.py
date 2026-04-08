"""
Définition du layout Dash — structure HTML/composants.
"""

from dash import dcc, html

from .config import PALETTE
from .data import (
    TOTAL_RECIPES,
    TOTAL_WITH_IMAGE,
    TOTAL_WITH_NUTRITION,
    AVG_KCAL,
    AVG_COOK_MIN,
    PCT_QUICK,
    PCT_A_B,
)

# Constantes de formulaires
NUTRI_OPTIONS = [
    {"label": "A", "value": "A"},
    {"label": "B", "value": "B"},
    {"label": "C", "value": "C"},
    {"label": "D", "value": "D"},
    {"label": "E", "value": "E"},
]

COOK_OPTIONS = [
    {"label": "⚡ Rapide", "value": "rapide"},
    {"label": "⏱ Moyen", "value": "moyen"},
    {"label": "🕐 Long", "value": "long"},
]


# Helpers
def card(children, className=""):
    """Génère un conteneur 'card' standardisé."""
    return html.Div(children, className=f"card {className}".strip())


def filter_badge(label, value, color):
    """Badge de filtre actif affiché dans la bannière de sélection."""
    return html.Span(
        [label, html.Span(" ×", className="filter-badge-close")],
        id={"type": "filter-badge", "value": value},
        className="filter-badge",
        # La couleur est gardée en inline car c'est une variable dynamique
        style={"backgroundColor": color},
    )


# Composants partiels
def build_filter_panel():
    """Génère le panneau global de filtres du dashboard."""
    return card(
        className="filter-panel",
        children=[
            html.Div(
                className="filter-container",
                children=[
                    # Titre panneau
                    html.Div(
                        className="filter-header",
                        children=[
                            html.Span("🔧", className="filter-icon"),
                            html.Span("Filtres", className="filter-title"),
                        ],
                    ),
                    # Nutri-Score
                    html.Div(
                        [
                            html.Label("Nutri-Score", className="filter-label"),
                            dcc.Checklist(
                                id="filter-nutri",
                                options=NUTRI_OPTIONS,
                                value=[],
                                inline=True,
                                inputClassName="checklist-input",
                                labelClassName="checklist-label",
                            ),
                        ]
                    ),
                    # Temps de cuisson
                    html.Div(
                        [
                            html.Label("Temps de cuisson", className="filter-label"),
                            dcc.Checklist(
                                id="filter-cook",
                                options=COOK_OPTIONS,
                                value=[],
                                inline=True,
                                inputClassName="checklist-input",
                                labelClassName="checklist-label checklist-label-normal",
                            ),
                        ]
                    ),
                    # Slider kcal
                    html.Div(
                        className="filter-slider",
                        children=[
                            html.Label(
                                "Énergie : toutes",
                                id="filter-kcal-label",
                                className="filter-label",
                            ),
                            dcc.RangeSlider(
                                id="filter-kcal",
                                min=0,
                                max=3500,
                                step=50,
                                value=[0, 3500],
                                marks={
                                    0: "0",
                                    500: "500",
                                    1000: "1k",
                                    2000: "2k",
                                    3500: "3500",
                                },
                                tooltip={"placement": "bottom", "always_visible": False},
                                allowCross=False,
                            ),
                        ],
                    ),
                    # Bouton reset
                    html.Button(
                        "✕ Réinitialiser",
                        id="btn-reset-filters",
                        n_clicks=0,
                        className="btn-reset",
                    ),
                ],
            ),
            # Bandeau "filtres actifs"
            html.Div(id="active-filters-display", className="active-filters"),
        ],
    )


# Layout principal
def build_layout():
    """Construit la structure de la page principale."""

    # Formatage des nombres pour l'affichage (espaces insécables)
    fmt_tot = f"{TOTAL_RECIPES:,}".replace(",", "\u202f")
    fmt_img = f"{TOTAL_WITH_IMAGE:,}".replace(",", "\u202f")
    fmt_nut = f"{TOTAL_WITH_NUTRITION:,}".replace(",", "\u202f")
    fmt_no_img = f"{TOTAL_RECIPES - TOTAL_WITH_IMAGE:,}".replace(",", "\u202f")

    return html.Div(
        className="main-layout",
        children=[
            # --- State Management (Stores) ---
            dcc.Store(id="store-selected-recipe-id", data=None),
            dcc.Store(id="store-recipe-idx", data=0),
            dcc.Store(id="store-recipe-image-urls"),
            dcc.Store(
                id="store-filters",
                data={
                    "nutri_scores": [],
                    "cook_cats": [],
                    "kcal_min": 0,
                    "kcal_max": 3500,
                },
            ),
            dcc.Interval(
                id="init-interval", interval=1, n_intervals=0, max_intervals=1
            ),

            # --- Ligne 1 : Titre / Image / KPIs ---
            html.Div(
                className="grid-row-1",
                children=[
                    # Bloc gauche : Intro & Recherche
                    card(
                        className="card-p-22 card-col",
                        children=[
                            html.H1("Le Grand Livre\ndes Recettes", className="main-title"),
                            html.P(
                                "Un catalogue culinaire enrichi par IA : recettes du "
                                "monde entier, analysées, traduites et scorées.",
                                className="main-subtitle",
                            ),
                            html.Div(
                                className="search-container",
                                children=[
                                    dcc.Input(
                                        id="search-input",
                                        type="text",
                                        placeholder="Rechercher une recette…",
                                        debounce=False,
                                        n_submit=0,
                                        className="search-input",
                                    ),
                                    html.Button(
                                        "🔍 Rechercher",
                                        id="btn-search",
                                        n_clicks=0,
                                        className="btn-primary",
                                    ),
                                ],
                            ),
                            dcc.Loading(
                                id="loading-search",
                                type="circle",
                                color=PALETTE["accent1"],
                                parent_style={
                                    "marginTop": "10px",
                                    "minHeight": "32px",
                                    "position": "relative",
                                },
                                overlay_style={
                                    "visibility": "visible",
                                    "display": "flex",
                                    "alignItems": "center",
                                    "justifyContent": "center",
                                    "height": "100%",
                                    "width": "100%",
                                },
                                children=html.Div(
                                    id="search-results", className="search-results"
                                ),
                            ),
                        ],
                    ),

                    # Bloc centre : Image recette dynamique
                    card(
                        className="card-p-0 card-hidden card-col",
                        children=[
                            dcc.Loading(
                                id="loading-recipe",
                                type="circle",
                                color=PALETTE["accent1"],
                                target_components={"recipe-image-container": "children"},
                                parent_style={
                                    "flex": "1",
                                    "width": "100%",
                                    "maxHeight": "400px",
                                    "overflow": "hidden",
                                    "display": "flex",
                                    "flexDirection": "column",
                                },
                                overlay_style={
                                    "visibility": "visible",
                                    "display": "flex",
                                    "alignItems": "center",
                                    "justifyContent": "center",
                                    "width": "100%",
                                    "height": "100%",
                                    "backgroundColor": "rgba(255,255,255,0.6)",
                                    "zIndex": "10",
                                },
                                children=html.Div(
                                    id="recipe-image-container",
                                    className="recipe-image-container",
                                ),
                            ),
                            html.Div(
                                className="recipe-header",
                                children=[
                                    html.P(id="recipe-title", className="recipe-title"),
                                    html.Button(
                                        "🎲 Aléatoire",
                                        id="btn-random-recipe",
                                        n_clicks=0,
                                        className="btn-primary btn-random",
                                    ),
                                ],
                            ),
                        ],
                    ),

                    # Bloc droit : Métriques globales
                    card(
                        className="card-p-22",
                        children=[
                            html.P("Nombre de recettes total", className="kpi-title"),
                            html.H2(fmt_tot, className="kpi-main-value"),
                            html.Hr(className="kpi-hr"),
                            html.Div([
                                html.Span(fmt_img, className="kpi-val-lg kpi-val-a2"),
                                html.Span(" avec image", className="kpi-sub"),
                            ]),
                            html.Div(
                                className="kpi-row",
                                children=[
                                    html.Span(fmt_nut, className="kpi-val-lg kpi-val-a4"),
                                    html.Span(" avec nutrition", className="kpi-sub"),
                                ],
                            ),
                            html.Hr(className="kpi-hr"),
                            html.Div(
                                className="kpi-row",
                                children=[
                                    html.Span(fmt_no_img, className="kpi-val-lg kpi-val-a3"),
                                    html.Span(" sans image", className="kpi-sub"),
                                ],
                            ),
                        ],
                    ),
                ],
            ),

            # --- Ligne 2 : Instructions & Ingrédients ---
            html.Div(
                className="grid-row-2",
                children=[
                    card(
                        className="card-col",
                        children=[
                            html.Div(
                                className="instructions-grid",
                                children=[
                                    html.Div([
                                        html.H3("Instructions", className="section-h3 section-h3-mb4"),
                                        html.P("(traduction IA)", className="section-meta section-meta-mb12"),
                                        html.Div(id="recipe-instructions-short", className="instructions-short"),
                                    ]),
                                    html.Div(id="recipe-instructions-content", className="instructions-content"),
                                ],
                            ),
                        ],
                    ),
                    card(
                        className="card-col",
                        children=[
                            html.H3("Ingrédients", className="section-h3 section-h3-mb12"),
                            html.P("(liste)", className="section-meta section-meta-mt8"),
                            html.Ul(id="ingredients-list", className="ingredients-list"),
                        ],
                    ),
                ],
            ),

            # --- Ligne 2b : Bannière Statistiques ---
            html.Div(
                className="stats-banner",
                children=[
                    html.Div(
                        className="stats-col-right-border",
                        children=[
                            html.Div(f"{int(AVG_KCAL):,}".replace(",", "\u202f"), className="stats-val stats-val-a1"),
                            html.Div("kcal moy. / recette", className="stats-label"),
                        ],
                    ),
                    html.Div(
                        className="stats-col-right-border",
                        children=[
                            html.Div(f"{int(PCT_QUICK)} %", className="stats-val stats-val-a2"),
                            html.Div("recettes rapides", className="stats-label"),
                        ],
                    ),
                    html.Div(
                        className="stats-col-center",
                        children=[
                            html.Div("📖", className="stats-icon"),
                            html.H3("Statistiques du Livre", className="stats-h3"),
                            html.P("Vue d'ensemble du catalogue", className="stats-p"),
                        ],
                    ),
                    html.Div(
                        className="stats-col-left-border",
                        children=[
                            html.Div(f"{int(PCT_A_B)} %", className="stats-val stats-val-a2"),
                            html.Div("Nutri-Score A ou B", className="stats-label"),
                        ],
                    ),
                    html.Div(
                        className="stats-col-left-border",
                        children=[
                            html.Div(f"{int(AVG_COOK_MIN)} min", className="stats-val stats-val-a4"),
                            html.Div("temps moy. de cuisson", className="stats-label"),
                        ],
                    ),
                ],
            ),

            # --- Panneau de Filtres ---
            build_filter_panel(),

            # --- Ligne 3 : Histogramme & Camembert ---
            html.Div(
                className="grid-row-3",
                children=[
                    card(
                        dcc.Graph(
                            id="graph-kcal-hist",
                            figure={},
                            config={"displayModeBar": False},
                            className="graph-220",
                        )
                    ),
                    card([
                        dcc.Graph(
                            id="graph-nutri-pie",
                            figure={},
                            config={"displayModeBar": False},
                            className="graph-220",
                        ),
                        html.P("Détail nutritionnel des recettes", className="pie-text"),
                    ]),
                ],
            ),

            # --- Ligne 4 : Bar Charts Nutri-Score & Cuisson ---
            html.Div(
                className="grid-row-4",
                children=[
                    card(
                        dcc.Graph(
                            id="graph-nutri-bar",
                            figure={},
                            config={"displayModeBar": False},
                            className="graph-220",
                        )
                    ),
                    card(
                        dcc.Graph(
                            id="graph-cook-times",
                            figure={},
                            config={"displayModeBar": False},
                            className="graph-220",
                        )
                    ),
                ],
            ),

            # --- Ligne 5 : Courbes & Scatter plots ---
            html.Div(
                className="grid-row-4",
                children=[
                    card(
                        dcc.Graph(
                            id="graph-cook-curve",
                            figure={},
                            config={"displayModeBar": False},
                            className="graph-280",
                        )
                    ),
                    card(
                        dcc.Graph(
                            id="graph-scatter-kcal",
                            figure={},
                            config={"displayModeBar": False},
                            className="graph-280",
                        )
                    ),
                ],
            ),

            # --- Ligne 6 : Tops Ingrédients & Tags ---
            html.Div(
                className="grid-row-5",
                children=[
                    card(
                        dcc.Graph(
                            id="graph-ingredients-top",
                            figure={},
                            config={"displayModeBar": False},
                            className="graph-320",
                        )
                    ),
                    card(
                        dcc.Graph(
                            id="graph-tags-top",
                            figure={},
                            config={"displayModeBar": False},
                            className="graph-320",
                        )
                    ),
                ],
            ),
        ],
    )

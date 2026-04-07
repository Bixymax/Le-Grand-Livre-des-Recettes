"""
Définition du layout Dash — structure HTML/composants uniquement.

Nouveautés v2 (dashboard dynamique) :
  - Panneau de filtres globaux (Nutri-Score, temps de cuisson, plage kcal)
  - Store dcc.Store("store-filters") pour propager les filtres à tous les graphiques
  - Nouveau graphique Top ingrédients (graph-ingredients-top)
"""

from dash import dcc, html

from .config import PALETTE
from .data import (
    TOTAL_RECIPES, TOTAL_WITH_IMAGE, TOTAL_WITH_NUTRITION,
    AVG_KCAL, AVG_COOK_MIN, PCT_QUICK, PCT_A_B,
)

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def card(children, style=None):
    base = {
        "background": PALETTE["card"],
        "border": f"1px solid {PALETTE['border']}",
        "borderRadius": "12px",
        "padding": "18px",
        "boxShadow": "0 1px 4px rgba(0,0,0,0.06)",
    }
    if style:
        base.update(style)
    return html.Div(children, style=base)


def filter_badge(label, value, color):
    """Badge de filtre actif (affiché dans la bannière de filtres actifs)."""
    return html.Span(
        [label, html.Span(" ×", style={"marginLeft": "4px", "opacity": "0.7"})],
        id={"type": "filter-badge", "value": value},
        style={
            "backgroundColor": color, "color": "white",
            "padding": "2px 8px", "borderRadius": "12px",
            "fontSize": "0.7rem", "fontWeight": "600",
            "cursor": "pointer", "marginRight": "4px",
        },
    )


# ---------------------------------------------------------------------------
# Panneau de filtres
# ---------------------------------------------------------------------------

def build_filter_panel():
    return card([
        html.Div(
            style={"display": "flex", "alignItems": "center", "gap": "24px", "flexWrap": "wrap"},
            children=[
                # Titre panneau
                html.Div([
                    html.Span("🔧", style={"fontSize": "1rem", "marginRight": "6px"}),
                    html.Span("Filtres", style={
                        "fontFamily": "'Playfair Display', Georgia, serif",
                        "fontWeight": "700", "fontSize": "0.95rem",
                        "color": PALETTE["text"],
                    }),
                ], style={"whiteSpace": "nowrap"}),

                # Nutri-Score
                html.Div([
                    html.Label("Nutri-Score", style={
                        "fontSize": "0.7rem", "color": PALETTE["muted"],
                        "textTransform": "uppercase", "letterSpacing": "0.06em",
                        "display": "block", "marginBottom": "4px",
                    }),
                    dcc.Checklist(
                        id="filter-nutri",
                        options=NUTRI_OPTIONS,
                        value=[],
                        inline=True,
                        inputStyle={"marginRight": "3px"},
                        labelStyle={
                            "marginRight": "8px", "fontSize": "0.8rem",
                            "fontWeight": "600", "cursor": "pointer",
                            "color": PALETTE["text"],
                        },
                    ),
                ]),

                # Temps de cuisson
                html.Div([
                    html.Label("Temps de cuisson", style={
                        "fontSize": "0.7rem", "color": PALETTE["muted"],
                        "textTransform": "uppercase", "letterSpacing": "0.06em",
                        "display": "block", "marginBottom": "4px",
                    }),
                    dcc.Checklist(
                        id="filter-cook",
                        options=COOK_OPTIONS,
                        value=[],
                        inline=True,
                        inputStyle={"marginRight": "3px"},
                        labelStyle={
                            "marginRight": "8px", "fontSize": "0.8rem",
                            "cursor": "pointer", "color": PALETTE["text"],
                        },
                    ),
                ]),

                # Slider kcal
                html.Div([
                    html.Label(
                        id="filter-kcal-label",
                        children="Énergie : toutes",
                        style={
                            "fontSize": "0.7rem", "color": PALETTE["muted"],
                            "textTransform": "uppercase", "letterSpacing": "0.06em",
                            "display": "block", "marginBottom": "4px",
                        },
                    ),
                    dcc.RangeSlider(
                        id="filter-kcal",
                        min=0, max=3500, step=50,
                        value=[0, 3500],
                        marks={0: "0", 500: "500", 1000: "1k", 2000: "2k", 3500: "3500"},
                        tooltip={"placement": "bottom", "always_visible": False},
                        allowCross=False,
                    ),
                ], style={"minWidth": "220px", "flex": "1"}),

                # Bouton reset
                html.Button(
                    "✕ Réinitialiser",
                    id="btn-reset-filters",
                    n_clicks=0,
                    style={
                        "backgroundColor": "transparent",
                        "border": f"1px solid {PALETTE['border']}",
                        "borderRadius": "6px", "padding": "5px 12px",
                        "cursor": "pointer", "fontSize": "0.75rem",
                        "color": PALETTE["muted"], "fontFamily": "'Lora', Georgia, serif",
                        "whiteSpace": "nowrap", "transition": "all 0.2s",
                        "marginLeft": "auto",
                    },
                ),
            ],
        ),

        # Bandeau "filtres actifs"
        html.Div(
            id="active-filters-display",
            style={"marginTop": "8px", "minHeight": "20px", "display": "flex", "flexWrap": "wrap", "gap": "4px"},
        ),
    ], style={"marginBottom": "16px", "padding": "14px 20px"})


# ---------------------------------------------------------------------------
# Layout principal
# ---------------------------------------------------------------------------

def build_layout():
    return html.Div(
        style={
            "backgroundColor": PALETTE["bg"], "minHeight": "100vh",
            "fontFamily": "'Lora', Georgia, serif", "color": PALETTE["text"],
            "padding": "20px",
        },
        children=[
            # --- Stores ---
            dcc.Store(id="store-selected-recipe-id", data=None),
            dcc.Store(id="store-recipe-idx", data=0),
            dcc.Store(id="store-recipe-image-urls"),

            # Store filtres — partagé par tous les graphiques
            dcc.Store(id="store-filters", data={
                "nutri_scores": [],
                "cook_cats": [],
                "kcal_min": 0,
                "kcal_max": 3500,
            }),

            dcc.Interval(id="init-interval", interval=1, n_intervals=0, max_intervals=1),

            # ----------------------------------------------------------------
            # Ligne 1 : Titre / Image / KPIs
            # ----------------------------------------------------------------
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "1fr 1.4fr 1fr",
                    "gap": "16px", "marginBottom": "16px",
                    "alignItems": "stretch", "minHeight": "360px",
                },
                children=[

                    # Carte gauche : titre + recherche
                    card([
                        html.H1("Le Grand Livre\ndes Recettes", style={
                            "fontFamily": "'Playfair Display', Georgia, serif",
                            "fontSize": "1.85rem", "fontWeight": "700",
                            "lineHeight": "1.25", "margin": "0 0 14px 0",
                            "whiteSpace": "pre-line",
                        }),
                        html.P(
                            "Un catalogue culinaire enrichi par IA : recettes du monde entier, "
                            "analysées, traduites et scorées nutritionnellement.",
                            style={
                                "fontSize": "0.82rem", "color": PALETTE["muted"],
                                "lineHeight": "1.6", "margin": "0", "flex": "1",
                            },
                        ),
                        html.Div(
                            style={
                                "display": "flex", "gap": "8px",
                                "alignItems": "center", "marginTop": "auto", "paddingTop": "16px",
                            },
                            children=[
                                dcc.Input(
                                    id="search-input", type="text",
                                    placeholder="Rechercher une recette…",
                                    debounce=False, n_submit=0,
                                    style={
                                        "flex": "1", "padding": "6px 12px",
                                        "border": f"1px solid {PALETTE['border']}",
                                        "borderRadius": "6px", "fontSize": "0.75rem",
                                        "fontFamily": "'Lora', Georgia, serif",
                                        "backgroundColor": PALETTE["bg"], "color": PALETTE["text"],
                                        "outline": "none",
                                        "boxShadow": "inset 0 1px 3px rgba(0,0,0,0.06)",
                                        "height": "30px", "boxSizing": "border-box",
                                    },
                                ),
                                html.Button("🔍 Rechercher", id="btn-search", n_clicks=0, style={
                                    "backgroundColor": PALETTE["accent1"], "color": "white",
                                    "border": "none", "borderRadius": "6px",
                                    "padding": "6px 12px", "cursor": "pointer",
                                    "fontWeight": "600", "fontSize": "0.75rem",
                                    "fontFamily": "'Lora', Georgia, serif",
                                    "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                                    "whiteSpace": "nowrap", "transition": "background 0.2s",
                                    "height": "30px", "boxSizing": "border-box",
                                }),
                            ],
                        ),
                        dcc.Loading(
                            id="loading-search",
                            type="circle",
                            color=PALETTE["accent1"],
                            parent_style={"marginTop": "10px", "minHeight": "32px", "position": "relative"},
                            overlay_style={
                                "visibility": "visible", "display": "flex",
                                "alignItems": "center", "justifyContent": "center",
                                "height": "100%", "width": "100%",
                            },
                            children=html.Div(
                                id="search-results",
                                style={
                                    "fontSize": "0.78rem", "color": PALETTE["text"],
                                    "maxHeight": "160px", "overflowY": "auto", "lineHeight": "1.7",
                                },
                            ),
                        ),
                    ], style={
                        "padding": "22px", "display": "flex",
                        "flexDirection": "column", "height": "100%", "boxSizing": "border-box",
                    }),

                    # Carte centre : image + titre recette
                    card([
                        dcc.Loading(
                            id="loading-recipe",
                            type="circle",
                            color=PALETTE["accent1"],
                            target_components={"recipe-image-container": "children"},
                            parent_style={
                                "flex": "1", "width": "100%", "maxHeight": "400px",
                                "overflow": "hidden", "display": "flex", "flexDirection": "column",
                            },
                            overlay_style={
                                "visibility": "visible", "display": "flex",
                                "alignItems": "center", "justifyContent": "center",
                                "width": "100%", "height": "100%",
                                "backgroundColor": "rgba(255,255,255,0.6)", "zIndex": "10",
                            },
                            children=html.Div(
                                id="recipe-image-container",
                                style={
                                    "width": "100%", "flex": "1", "overflow": "hidden",
                                    "backgroundColor": PALETTE["bg"], "maxHeight": "400px",
                                },
                            ),
                        ),
                        html.Div(
                            style={
                                "display": "flex", "justifyContent": "space-between",
                                "alignItems": "center", "padding": "10px 14px",
                                "borderTop": f"1px solid {PALETTE['border']}", "flexShrink": "0",
                            },
                            children=[
                                html.P(
                                    id="recipe-title",
                                    style={
                                        "margin": "0", "fontWeight": "600",
                                        "fontSize": "0.95rem", "overflow": "hidden",
                                        "whiteSpace": "nowrap", "textOverflow": "ellipsis",
                                        "flex": "1", "paddingRight": "12px",
                                        "fontFamily": "'Playfair Display', Georgia, serif",
                                    },
                                ),
                                html.Button("🎲 Aléatoire", id="btn-random-recipe", n_clicks=0, style={
                                    "backgroundColor": PALETTE["accent1"], "color": "white",
                                    "border": "none", "borderRadius": "6px",
                                    "padding": "6px 12px", "cursor": "pointer",
                                    "fontWeight": "600", "fontSize": "0.75rem",
                                    "fontFamily": "'Lora', Georgia, serif",
                                    "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                                    "whiteSpace": "nowrap", "transition": "background 0.2s",
                                    "flexShrink": "0",
                                }),
                            ],
                        ),
                    ], style={"padding": "0", "overflow": "hidden", "display": "flex", "flexDirection": "column", "height": "100%"}),

                    # Carte droite : KPIs
                    card([
                        html.P("Nombre de recettes total", style={
                            "fontSize": "0.78rem", "color": PALETTE["muted"],
                            "textTransform": "uppercase", "letterSpacing": "0.08em",
                            "marginBottom": "12px",
                        }),
                        html.H2(
                            f"{TOTAL_RECIPES:,}".replace(",", "\u202f"),
                            style={
                                "fontFamily": "'Playfair Display', Georgia, serif",
                                "fontSize": "2.6rem", "fontWeight": "700",
                                "margin": "0", "color": PALETTE["accent1"],
                            },
                        ),
                        html.Hr(style={"border": "none", "borderTop": f"2px solid {PALETTE['border']}", "margin": "18px 0"}),
                        html.Div([
                            html.Span(f"{TOTAL_WITH_IMAGE:,}".replace(",", "\u202f"),
                                      style={"fontSize": "1.1rem", "fontWeight": "600", "color": PALETTE["accent2"]}),
                            html.Span(" avec image", style={"fontSize": "0.78rem", "color": PALETTE["muted"], "marginLeft": "4px"}),
                        ]),
                        html.Div([
                            html.Span(f"{TOTAL_WITH_NUTRITION:,}".replace(",", "\u202f"),
                                      style={"fontSize": "1.1rem", "fontWeight": "600", "color": PALETTE["accent4"]}),
                            html.Span(" avec nutrition", style={"fontSize": "0.78rem", "color": PALETTE["muted"], "marginLeft": "4px"}),
                        ], style={"marginTop": "6px"}),
                        html.Hr(style={"border": "none", "borderTop": f"2px solid {PALETTE['border']}", "margin": "18px 0"}),
                        html.Div([
                            html.Span(f"{TOTAL_RECIPES - TOTAL_WITH_IMAGE:,}".replace(",", "\u202f"),
                                      style={"fontSize": "1.1rem", "fontWeight": "600", "color": PALETTE["accent3"]}),
                            html.Span(" sans image", style={"fontSize": "0.78rem", "color": PALETTE["muted"], "marginLeft": "4px"}),
                        ], style={"marginTop": "6px"}),
                    ], style={"padding": "22px"}),
                ],
            ),

            # ----------------------------------------------------------------
            # Ligne 2 : Instructions + Ingrédients
            # ----------------------------------------------------------------
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "3fr 1fr", "gap": "16px", "marginBottom": "16px", "alignItems": "stretch"},
                children=[
                    card([
                        html.Div(
                            style={"display": "grid", "gridTemplateColumns": "auto 1fr", "gap": "24px", "height": "100%"},
                            children=[
                                html.Div([
                                    html.H3("Instructions", style={
                                        "fontFamily": "'Playfair Display'", "fontSize": "0.95rem",
                                        "fontWeight": "600", "marginBottom": "4px",
                                    }),
                                    html.P("(traduction IA)", style={
                                        "fontSize": "0.72rem", "color": PALETTE["muted"],
                                        "fontStyle": "italic", "marginBottom": "12px",
                                    }),
                                    html.Div(id="recipe-instructions-short", style={
                                        "fontSize": "0.8rem", "color": PALETTE["muted"],
                                        "lineHeight": "1.7", "whiteSpace": "nowrap",
                                    }),
                                ]),
                                html.Div(id="recipe-instructions-content", style={
                                    "columnCount": 2, "columnGap": "24px",
                                    "columnRule": f"1px solid {PALETTE['border']}",
                                    "borderLeft": f"1px solid {PALETTE['border']}",
                                    "paddingLeft": "24px", "fontSize": "0.8rem",
                                    "lineHeight": "1.75", "color": PALETTE["text"],
                                }),
                            ],
                        ),
                    ], style={"height": "100%", "boxSizing": "border-box"}),

                    card([
                        html.H3("Ingrédients", style={
                            "fontFamily": "'Playfair Display'", "fontSize": "0.95rem",
                            "fontWeight": "600", "marginBottom": "12px",
                        }),
                        html.P("(liste)", style={
                            "fontSize": "0.72rem", "color": PALETTE["muted"],
                            "fontStyle": "italic", "marginTop": "-8px", "marginBottom": "10px",
                        }),
                        html.Ul(id="ingredients-list", style={
                            "listStyle": "none", "padding": "0", "margin": "0",
                            "fontSize": "0.8rem", "lineHeight": "2",
                        }),
                    ], style={"height": "100%", "boxSizing": "border-box"}),
                ],
            ),

            # ----------------------------------------------------------------
            # Ligne 2b : Bannière statistiques
            # ----------------------------------------------------------------
            html.Div(
                style={
                    "background": (
                        f"linear-gradient(135deg, {PALETTE['accent1']}18 0%, "
                        f"{PALETTE['accent4']}12 50%, {PALETTE['accent2']}18 100%)"
                    ),
                    "border": f"1px solid {PALETTE['border']}",
                    "borderRadius": "12px", "padding": "14px 24px",
                    "marginBottom": "16px", "display": "flex",
                    "alignItems": "center", "gap": "0",
                    "boxShadow": "0 1px 4px rgba(0,0,0,0.06)",
                },
                children=[
                    html.Div(style={
                        "textAlign": "center", "flex": "1",
                        "borderRight": f"1px solid {PALETTE['border']}",
                        "paddingRight": "16px", "marginRight": "16px",
                    }, children=[
                        html.Div(f"{int(AVG_KCAL):,}".replace(",", "\u202f"), style={
                            "fontFamily": "'Playfair Display', Georgia, serif",
                            "fontSize": "1.5rem", "fontWeight": "700",
                            "color": PALETTE["accent1"], "lineHeight": "1.1",
                        }),
                        html.Div("kcal moy. / recette", style={
                            "fontSize": "0.68rem", "color": PALETTE["muted"],
                            "textTransform": "uppercase", "letterSpacing": "0.06em", "marginTop": "3px",
                        }),
                    ]),
                    html.Div(style={
                        "textAlign": "center", "flex": "1",
                        "borderRight": f"1px solid {PALETTE['border']}",
                        "paddingRight": "16px", "marginRight": "16px",
                    }, children=[
                        html.Div(f"{int(PCT_QUICK)} %", style={
                            "fontFamily": "'Playfair Display', Georgia, serif",
                            "fontSize": "1.5rem", "fontWeight": "700",
                            "color": PALETTE["accent2"], "lineHeight": "1.1",
                        }),
                        html.Div("recettes rapides", style={
                            "fontSize": "0.68rem", "color": PALETTE["muted"],
                            "textTransform": "uppercase", "letterSpacing": "0.06em", "marginTop": "3px",
                        }),
                    ]),
                    html.Div(style={"flex": "2", "textAlign": "center", "padding": "0 16px"}, children=[
                        html.Div("📖", style={"fontSize": "1.2rem", "marginBottom": "2px"}),
                        html.H3("Statistiques du Livre", style={
                            "fontFamily": "'Playfair Display', Georgia, serif",
                            "fontSize": "1.05rem", "fontWeight": "700",
                            "margin": "0 0 2px 0", "color": PALETTE["text"], "whiteSpace": "nowrap",
                        }),
                        html.P("Vue d'ensemble du catalogue", style={
                            "fontSize": "0.68rem", "color": PALETTE["muted"],
                            "margin": "0", "fontStyle": "italic", "letterSpacing": "0.04em",
                        }),
                    ]),
                    html.Div(style={
                        "textAlign": "center", "flex": "1",
                        "borderLeft": f"1px solid {PALETTE['border']}",
                        "paddingLeft": "16px", "marginLeft": "16px",
                    }, children=[
                        html.Div(f"{int(PCT_A_B)} %", style={
                            "fontFamily": "'Playfair Display', Georgia, serif",
                            "fontSize": "1.5rem", "fontWeight": "700",
                            "color": PALETTE["accent2"], "lineHeight": "1.1",
                        }),
                        html.Div("Nutri-Score A ou B", style={
                            "fontSize": "0.68rem", "color": PALETTE["muted"],
                            "textTransform": "uppercase", "letterSpacing": "0.06em", "marginTop": "3px",
                        }),
                    ]),
                    html.Div(style={
                        "textAlign": "center", "flex": "1",
                        "borderLeft": f"1px solid {PALETTE['border']}",
                        "paddingLeft": "16px", "marginLeft": "16px",
                    }, children=[
                        html.Div(f"{int(AVG_COOK_MIN)} min", style={
                            "fontFamily": "'Playfair Display', Georgia, serif",
                            "fontSize": "1.5rem", "fontWeight": "700",
                            "color": PALETTE["accent4"], "lineHeight": "1.1",
                        }),
                        html.Div("temps moy. de cuisson", style={
                            "fontSize": "0.68rem", "color": PALETTE["muted"],
                            "textTransform": "uppercase", "letterSpacing": "0.06em", "marginTop": "3px",
                        }),
                    ]),
                ],
            ),

            # ----------------------------------------------------------------
            # PANNEAU DE FILTRES (nouveau)
            # ----------------------------------------------------------------
            build_filter_panel(),

            # ----------------------------------------------------------------
            # Ligne 3 : Histogramme kcal + Pie nutritionnel
            # ----------------------------------------------------------------
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "3fr 1.5fr", "gap": "16px", "marginBottom": "16px"},
                children=[
                    card([dcc.Graph(
                        id="graph-kcal-hist", figure={},
                        config={"displayModeBar": False}, style={"height": "220px"},
                    )]),
                    card([
                        dcc.Graph(
                            id="graph-nutri-pie", figure={},
                            config={"displayModeBar": False}, style={"height": "220px"},
                        ),
                        html.P("recipes nutrition detail", style={
                            "textAlign": "center", "fontSize": "0.72rem",
                            "color": PALETTE["muted"], "margin": "0",
                        }),
                    ]),
                ],
            ),

            # ----------------------------------------------------------------
            # Ligne 4 : Bar Nutri-Score + Bar temps de cuisson
            # ----------------------------------------------------------------
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 2fr", "gap": "16px", "marginBottom": "16px"},
                children=[
                    card([dcc.Graph(
                        id="graph-nutri-bar", figure={},
                        config={"displayModeBar": False}, style={"height": "220px"},
                    )]),
                    card([dcc.Graph(
                        id="graph-cook-times", figure={},
                        config={"displayModeBar": False}, style={"height": "220px"},
                    )]),
                ],
            ),

            # ----------------------------------------------------------------
            # Ligne 5 : Courbe cook_time + Scatter
            # ----------------------------------------------------------------
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 2fr", "gap": "16px", "marginBottom": "16px"},
                children=[
                    card([dcc.Graph(
                        id="graph-cook-curve", figure={},
                        config={"displayModeBar": False}, style={"height": "280px"},
                    )]),
                    card([dcc.Graph(
                        id="graph-scatter-kcal", figure={},
                        config={"displayModeBar": False}, style={"height": "280px"},
                    )]),
                ],
            ),

            # ----------------------------------------------------------------
            # Ligne 6 : Top ingrédients (nouveau graphique dynamique)
            # ----------------------------------------------------------------
            card([dcc.Graph(
                id="graph-ingredients-top", figure={},
                config={"displayModeBar": False}, style={"height": "320px"},
            )], style={"marginBottom": "16px"}),
        ],
    )
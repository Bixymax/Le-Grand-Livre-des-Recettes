"""
Définition du layout Dash — structure HTML/composants uniquement.
Aucun callback ici.

OPT P1 : les figures Plotly ne sont plus calculées ici.
         Elles sont initialisées vides (figure={}) et remplies par
         load_all_charts() dans callbacks.py via l'init-interval.
"""

from dash import dcc, html

from .config import PALETTE
from .data import (
    TOTAL_RECIPES, TOTAL_WITH_IMAGE, TOTAL_WITH_NUTRITION,
    AVG_KCAL, AVG_COOK_MIN, PCT_QUICK, PCT_A_B,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def card(children, style=None):
    """Encapsule les composants dans une carte stylisée."""
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
            # OPT P1 : max_intervals=1 → déclenche une seule fois le chargement des graphiques
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
                        # Résultats de recherche (inline — plus de modal)
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
                                id="search-results",
                                style={
                                    "fontSize": "0.78rem",
                                    "color": PALETTE["text"],
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
                            },
                            children=html.Div(
                                id="recipe-image-container",
                                style={
                                    "width": "100%",
                                    "flex": "1",
                                    "overflow": "hidden",
                                    "backgroundColor": PALETTE["bg"],
                                    "maxHeight": "400px",
                                },
                            ),
                        ),
                        # Bandeau titre + bouton aléatoire
                        html.Div(
                            style={
                                "display": "flex", "justifyContent": "space-between",
                                "alignItems": "center", "padding": "10px 14px",
                                "borderTop": f"1px solid {PALETTE['border']}",
                                "flexShrink": "0",
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
                    ], style={
                        "padding": "0", "overflow": "hidden",
                        "display": "flex", "flexDirection": "column", "height": "100%",
                    }),

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
                        html.Hr(style={
                            "border": "none",
                            "borderTop": f"2px solid {PALETTE['border']}", "margin": "18px 0",
                        }),
                        html.Div([
                            html.Span(
                                f"{TOTAL_WITH_IMAGE:,}".replace(",", "\u202f"),
                                style={"fontSize": "1.1rem", "fontWeight": "600",
                                       "color": PALETTE["accent2"]},
                            ),
                            html.Span(" avec image", style={
                                "fontSize": "0.78rem", "color": PALETTE["muted"], "marginLeft": "4px",
                            }),
                        ]),
                        html.Div([
                            html.Span(
                                f"{TOTAL_WITH_NUTRITION:,}".replace(",", "\u202f"),
                                style={"fontSize": "1.1rem", "fontWeight": "600",
                                       "color": PALETTE["accent4"]},
                            ),
                            html.Span(" avec nutrition", style={
                                "fontSize": "0.78rem", "color": PALETTE["muted"], "marginLeft": "4px",
                            }),
                        ], style={"marginTop": "6px"}),
                        html.Hr(style={
                            "border": "none",
                            "borderTop": f"2px solid {PALETTE['border']}", "margin": "18px 0",
                        }),
                        html.Div([
                            html.Span(
                                f"{TOTAL_RECIPES - TOTAL_WITH_IMAGE:,}".replace(",", "\u202f"),
                                style={"fontSize": "1.1rem", "fontWeight": "600",
                                       "color": PALETTE["accent3"]},
                            ),
                            html.Span(" sans image", style={
                                "fontSize": "0.78rem", "color": PALETTE["muted"], "marginLeft": "4px",
                            }),
                        ], style={"marginTop": "6px"}),
                    ], style={"padding": "22px"}),
                ],
            ),

            # ----------------------------------------------------------------
            # Ligne 2 : Instructions + Ingrédients
            # ----------------------------------------------------------------
            html.Div(
                style={
                    "display": "grid", "gridTemplateColumns": "3fr 1fr",
                    "gap": "16px", "marginBottom": "16px", "alignItems": "stretch",
                },
                children=[
                    card([
                        html.Div(
                            style={"display": "grid", "gridTemplateColumns": "auto 1fr",
                                   "gap": "24px", "height": "100%"},
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
                            "textTransform": "uppercase", "letterSpacing": "0.06em",
                            "marginTop": "3px",
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
                            "textTransform": "uppercase", "letterSpacing": "0.06em",
                            "marginTop": "3px",
                        }),
                    ]),
                    html.Div(style={"flex": "2", "textAlign": "center", "padding": "0 16px"}, children=[
                        html.Div("📖", style={"fontSize": "1.2rem", "marginBottom": "2px"}),
                        html.H3("Statistiques du Livre", style={
                            "fontFamily": "'Playfair Display', Georgia, serif",
                            "fontSize": "1.05rem", "fontWeight": "700",
                            "margin": "0 0 2px 0", "color": PALETTE["text"],
                            "whiteSpace": "nowrap",
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
                            "textTransform": "uppercase", "letterSpacing": "0.06em",
                            "marginTop": "3px",
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
                            "textTransform": "uppercase", "letterSpacing": "0.06em",
                            "marginTop": "3px",
                        }),
                    ]),
                ],
            ),

            # ----------------------------------------------------------------
            # Ligne 3 : Histogramme kcal + Pie nutritionnel
            # OPT P1 : figure={} — rempli par load_all_charts() au premier rendu
            # ----------------------------------------------------------------
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "3fr 1.5fr",
                       "gap": "16px", "marginBottom": "16px"},
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
                style={"display": "grid", "gridTemplateColumns": "1fr 2fr",
                       "gap": "16px", "marginBottom": "16px"},
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
                style={"display": "grid", "gridTemplateColumns": "1fr 2fr",
                       "gap": "16px", "marginBottom": "16px"},
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
        ],
    )

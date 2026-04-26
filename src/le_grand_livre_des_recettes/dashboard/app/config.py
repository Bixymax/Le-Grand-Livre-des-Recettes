"""
Constantes graphiques, palettes et configuration partagée.
"""

PALETTE = {
    "bg": "#FAFAF7", "card": "#FFFFFF", "border": "#E8E4DC", "text": "#2C2825",
    "muted": "#8A8480", "accent1": "#E07B39", "accent2": "#3D7A5F",
    "accent3": "#C94F4F", "accent4": "#4A6FA5", "accent5": "#9B7EBD",
}

NUTRI_COLORS = {
    "A": "#3D7A5F", "B": "#6BAE6A", "C": "#F2C14E",
    "D": "#E07B39", "E": "#C94F4F", "?": "#8A8480"
}

PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="'Playfair Display', Georgia, serif", color=PALETTE["text"], size=11),
    margin=dict(l=10, r=10, t=30, b=10), showlegend=False,
)

GOOGLE_FONTS = (
    "https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700"
    "&family=Lora:ital,wght@0,400;0,500;1,400&display=swap"
)
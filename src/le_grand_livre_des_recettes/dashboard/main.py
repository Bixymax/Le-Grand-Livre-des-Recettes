"""
Le Grand Livre des Recettes — point d'entrée.

Lancer avec :
    python main.py
"""

import dash

from app.config import GOOGLE_FONTS
from app.layout import build_layout
from app.callbacks import register_callbacks

app = dash.Dash(
    __name__,
    external_stylesheets=[GOOGLE_FONTS],
    title="Le Grand Livre des Recettes",
)

app.layout = build_layout()
register_callbacks(app)

if __name__ == "__main__":
    app.run(debug=True, port=8080)
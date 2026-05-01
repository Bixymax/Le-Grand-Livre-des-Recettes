"""
Utilitaires partagés entre les sources dlt.
"""

from __future__ import annotations

import re
import time


def normalize_title(title: str) -> str:
    """
    Normalise un titre de recette pour créer une clé de jointure robuste.

    Supprime la ponctuation, passe en minuscules et retire les espaces superflus.
    Indispensable pour fiabiliser les jointures entre datasets hétérogènes
    (MIT Recipe1M+ vs Kaggle Food.com).

    Parameters
    ----------
    title:
        Titre brut tel qu'il apparaît dans le fichier source.

    Returns
    -------
    str
        Titre normalisé, ex. ``"Chicken & Rice!"`` → ``"chicken  rice"``.
    """
    return re.sub(r"[^a-zA-Z0-9\s]", "", title or "").lower().strip()


def log_progress(name: str, count: int, t0: float, every: int = 50_000) -> None:
    """
    Affiche la progression de la lecture d'un fichier source tous les *every* enregistrements.

    Parameters
    ----------
    name:
        Nom de la ressource (ex. ``"layer1"``).
    count:
        Nombre d'enregistrements traités jusqu'ici.
    t0:
        Timestamp de début (``time.perf_counter()``).
    every:
        Intervalle d'affichage.
    """
    if count > 0 and count % every == 0:
        elapsed = time.perf_counter() - t0
        print(f"  [{name}] {count:,} enregistrements — {elapsed:.0f}s", flush=True)

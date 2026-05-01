"""Utilitaires partagés pour les sources DLT."""

from __future__ import annotations

import re
import time


def normalize_title(title: str) -> str:
    """Génère une clé de jointure normalisée à partir d'un titre brut.

    Args:
        title: Titre original de la recette.

    Returns:
        Titre nettoyé (minuscules, caractères alphanumériques et espaces uniquement).
    """
    return re.sub(r"[^a-zA-Z0-9\s]", "", title or "").lower().strip()


def log_progress(name: str, count: int, t0: float, every: int = 50_000) -> None:
    """Affiche la progression du traitement par lots sur la sortie standard.

    Args:
        name: Identifiant de la ressource en cours de traitement.
        count: Nombre d'enregistrements traités.
        t0: Timestamp initial de référence (généré via time.perf_counter()).
        every: Intervalle de déclenchement de l'affichage.
    """
    if count > 0 and count % every == 0:
        elapsed = time.perf_counter() - t0
        print(f"  [{name}] {count:,} enregistrements — {elapsed:.0f}s", flush=True)
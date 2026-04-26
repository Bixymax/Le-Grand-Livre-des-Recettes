"""
Fonctions d'enrichissement métier — pures, sans dépendance externe.

Ces fonctions s'appliquent sur les données **après** les jointures SQL.
Les maintenir séparées des sources permet de les tester unitairement
sans avoir besoin de DuckDB ou de fichiers JSON.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Nutri-Score
# Seuils identiques au pipeline Spark original.
# ---------------------------------------------------------------------------

_NUTRI_SCORE_THRESHOLDS: list[tuple[float, str]] = [
    (80.0,  "A"),
    (160.0, "B"),
    (270.0, "C"),
    (400.0, "D"),
]

def compute_nutri_score(energy_kcal: float | None) -> str | None:
    """
    Retourne un Nutri-Score simplifié (A–E) basé sur les kcal/100g.
    Retourne None si la valeur est manquante.

    Seuils :
        < 80   → A
        < 160  → B
        < 270  → C
        < 400  → D
        >= 400 → E
    """
    if energy_kcal is None:
        return None
    for threshold, score in _NUTRI_SCORE_THRESHOLDS:
        if energy_kcal < threshold:
            return score
    return "E"


# ---------------------------------------------------------------------------
# Catégorie de temps de cuisson
# ---------------------------------------------------------------------------

def compute_cook_time_category(cook_minutes: int | None) -> str:
    """
    Catégorise le temps de cuisson.
    Retourne "inconnu" si la valeur est None.

    Catégories :
        <= 30 min  → rapide
        <= 60 min  → moyen
        > 60 min   → long
        None       → inconnu
    """
    if cook_minutes is None:
        return "inconnu"
    if cook_minutes <= 30:
        return "rapide"
    if cook_minutes <= 60:
        return "moyen"
    return "long"


# ---------------------------------------------------------------------------
# Réconciliation des données nutritionnelles (coalesce)
# Reproduit F.coalesce(energy_kcal, kaggle_energy_kcal)
# ---------------------------------------------------------------------------

def coalesce_energy(
    mit_energy: float | None,
    kaggle_energy: float | None,
) -> float | None:
    """
    Priorité à la valeur MIT (plus précise, per 100g).
    Fallback sur Kaggle si MIT est null.
    """
    return mit_energy if mit_energy is not None else kaggle_energy
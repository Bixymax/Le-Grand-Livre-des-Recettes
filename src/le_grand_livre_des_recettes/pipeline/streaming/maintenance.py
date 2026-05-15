"""
Script de maintenance Delta Lake pour le flux temps réel.

À exécuter via un orchestrateur (Airflow, cron) ou un simple planificateur de tâches,
par exemple toutes les heures ou toutes les nuits.
"""

from pathlib import Path
from deltalake import DeltaTable
from le_grand_livre_des_recettes.pipeline import config as cfg


def run_maintenance():
    stream_path = Path(cfg.OUT_RECIPES_STREAM)

    if not stream_path.exists():
        print(f"[Maintenance] La table {stream_path} n'existe pas encore. Arrêt.")
        return

    print(f"[Maintenance] Démarrage sur {stream_path}...")
    dt = DeltaTable(str(stream_path))

    # 1. OPTIMIZE (Compactage)
    # Regroupe tous les petits fichiers Parquet de 30 secondes en gros fichiers optimisés
    print("[Maintenance] Compactage des petits fichiers (OPTIMIZE)...")
    dt.optimize.compact()

    # 2. VACUUM (Nettoyage)
    # Supprime les anciens petits fichiers qui ont été remplacés par l'opération de compactage
    print("[Maintenance] Nettoyage des fichiers orphelins (VACUUM)...")
    dt.vacuum()

    print("[Maintenance] Terminé avec succès ! DuckDB va adorer.")


if __name__ == "__main__":
    run_maintenance()
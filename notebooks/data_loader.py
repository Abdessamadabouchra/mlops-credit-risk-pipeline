"""Chargement du jeu de données gold pour les notebooks d'analyse.

1. Source principale : l'instantané gold versionné par DVC (data/gold_snapshot/gold.parquet), produit par la tâche
   Airflow `dvc_snapshot` (ou `dvc pull` / `.\\dvc_snapshot.ps1`).
2. Repli si le fichier est absent (projet cloné sans avoir lancé le pipeline) : un échantillon est généré avec le
   MEME générateur (generator/producer.py) puis nettoyé et enrichi avec les MEMES règles que les couches silver et
   gold (jobs/bronze_to_silver.py, jobs/features.py). Les notebooks restent ainsi exécutables sans l'infrastructure.
"""
import os
import random
import sys
import types
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
GOLD = ROOT / "data" / "gold_snapshot" / "gold.parquet"

CATEGORICAL = ["home_ownership", "loan_purpose"]
NUMERIC_RAW = ["age", "income", "employment_years", "loan_amount", "loan_term_months", "interest_rate",
               "credit_score", "num_open_accounts", "num_late_payments", "debt_to_income"]
NUMERIC_DERIVED = ["payment_to_income", "late_per_account"]
NUMERIC = NUMERIC_RAW + NUMERIC_DERIVED
TARGET = "label"


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Même calcul que jobs/features.py::add_features (version pandas)."""
    monthly_payment = df["loan_amount"] * (1 + df["interest_rate"] / 100) / df["loan_term_months"]
    df = df.copy()
    df["payment_to_income"] = monthly_payment / (df["income"] / 12)
    df["late_per_account"] = df["num_late_payments"] / (df["num_open_accounts"] + 1)
    return df


def _import_generator():
    """Importe generator/producer.py sans exiger kafka-python (seul make_event est utilisé)."""
    try:
        import kafka  # noqa: F401
    except ImportError:
        kafka = types.ModuleType("kafka")
        kafka.KafkaProducer = None
        errors = types.ModuleType("kafka.errors")
        errors.NoBrokersAvailable = Exception
        kafka.errors = errors
        sys.modules["kafka"], sys.modules["kafka.errors"] = kafka, errors
    sys.path.insert(0, str(ROOT / "generator"))
    import producer
    return producer


def generate_sample(n: int = 60_000, seed: int = 42) -> pd.DataFrame:
    """Échantillon reproductible : générateur du projet + règles de silver + variables de gold."""
    producer = _import_generator()
    random.seed(seed)
    rows = []
    for _ in range(n):
        e = producer.make_event()
        if random.random() < 0.02:          # mêmes lignes sales que le générateur
            e["income"] = None
        rows.append(e)
        if random.random() < 0.01:          # mêmes doublons
            rows.append(dict(e))
    raw = pd.DataFrame(rows)
    # règles de validation de la couche silver
    valid = (raw["income"].notna() & (raw["income"] > 0) & raw["age"].between(18, 100)
             & (raw["loan_amount"] > 0) & (raw["loan_term_months"] > 0)
             & raw["credit_score"].between(300, 850) & raw["default"].isin([0, 1]))
    silver = raw[valid].drop_duplicates("application_id")
    gold = add_features(silver).rename(columns={"default": TARGET})
    return gold[["application_id"] + NUMERIC + CATEGORICAL + [TARGET]].reset_index(drop=True)


def _n(x: int) -> str:
    return f"{x:,}".replace(",", " ")


def load_gold() -> tuple[pd.DataFrame, str]:
    """Retourne (dataframe gold, description de la source).

    La variable d'environnement EDA_FORCE_SAMPLE=1 force l'échantillon généré (utile pour tester le mode de repli)."""
    if GOLD.exists() and not os.environ.get("EDA_FORCE_SAMPLE"):
        df = pd.read_parquet(GOLD)
        return df, f"instantané gold versionné par DVC ({GOLD.relative_to(ROOT).as_posix()}), {_n(len(df))} lignes"
    df = generate_sample()
    return df, (f"échantillon généré ({_n(len(df))} lignes) : data/gold_snapshot/gold.parquet absent ; "
                "lancer le pipeline puis la tâche dvc_snapshot pour analyser les données réelles")

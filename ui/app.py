"""Streamlit front-end for the credit-risk API (POST /predict, /reload, GET /health)."""
import os
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")
TIMEOUT = 300   # the first prediction after a (re)load starts Spark inside the API and can take ~1 min

HOME_OWNERSHIP = ["RENT", "OWN", "MORTGAGE"]
LOAN_PURPOSE = ["car", "home", "education", "business", "debt"]

PROFILES = {
    "Profil sûr": dict(age=35, income=52000.0, employment_years=8, loan_amount=15000.0, loan_term_months=36,
                       interest_rate=9.5, credit_score=690, num_open_accounts=4, num_late_payments=0,
                       home_ownership="RENT", loan_purpose="car"),
    "Profil intermédiaire": dict(age=42, income=30000.0, employment_years=3, loan_amount=22000.0,
                                 loan_term_months=48, interest_rate=14.0, credit_score=600, num_open_accounts=7,
                                 num_late_payments=2, home_ownership="MORTGAGE", loan_purpose="debt"),
    "Profil risqué": dict(age=29, income=18000.0, employment_years=1, loan_amount=38000.0, loan_term_months=12,
                          interest_rate=21.0, credit_score=430, num_open_accounts=9, num_late_payments=6,
                          home_ownership="RENT", loan_purpose="business"),
}

st.set_page_config(page_title="Risque de crédit", page_icon="💳", layout="wide")

if "history" not in st.session_state:
    st.session_state.history = []
for k, v in PROFILES["Profil sûr"].items():
    st.session_state.setdefault(k, v)


def api_get(path):
    return requests.get(f"{API_URL}{path}", timeout=10).json()


def api_post(path, payload=None):
    r = requests.post(f"{API_URL}{path}", json=payload, timeout=TIMEOUT)
    if r.status_code >= 400:
        detail = r.json().get("detail", r.text) if "json" in r.headers.get("content-type", "") else r.text
        raise RuntimeError(f"{r.status_code} : {detail}")
    return r.json()


def load_profile(name):
    for k, v in PROFILES[name].items():
        st.session_state[k] = v


# ------------------------------------------------------------------ sidebar: API / model status
with st.sidebar:
    st.header("Modèle en production")
    try:
        health = api_get("/health")
        st.success("API disponible")
        version = health.get("model_version")
        st.metric("Version du champion", f"v{version}" if version else "pas encore chargé")
    except Exception as e:
        st.error(f"API injoignable ({API_URL})")
        st.caption(str(e)[:200])
    if st.button("Recharger le modèle champion", use_container_width=True):
        with st.spinner("Chargement du modèle depuis MLflow / MinIO…"):
            try:
                st.success(f"Modèle v{api_post('/reload')['model_version']} chargé")
            except Exception as e:
                st.error(str(e))
    st.divider()
    st.caption("Le modèle « champion » est choisi par le DAG Airflow (MLflow Model Registry, alias `champion`). "
               "Après une nouvelle promotion, cliquez sur « Recharger ».")

# ------------------------------------------------------------------ main: application form
st.title("💳 Évaluation du risque de crédit")
st.write("Saisissez une demande de prêt : le modèle renvoie la probabilité de défaut de paiement.")

cols = st.columns(len(PROFILES))
for col, name in zip(cols, PROFILES):
    col.button(f"Exemple : {name}", on_click=load_profile, args=(name,), use_container_width=True)

with st.form("application"):
    c1, c2, c3 = st.columns(3)
    with c1:
        st.subheader("Client")
        st.number_input("Âge", 18, 100, key="age")
        st.number_input("Revenu annuel", 1000.0, 1_000_000.0, step=1000.0, key="income")
        st.number_input("Années d'emploi", 0, 60, key="employment_years")
        st.selectbox("Logement", HOME_OWNERSHIP, key="home_ownership")
    with c2:
        st.subheader("Prêt")
        st.number_input("Montant demandé", 100.0, 1_000_000.0, step=500.0, key="loan_amount")
        st.selectbox("Durée (mois)", [12, 24, 36, 48, 60], key="loan_term_months")
        st.number_input("Taux d'intérêt (%)", 0.0, 40.0, step=0.5, key="interest_rate")
        st.selectbox("Objet du prêt", LOAN_PURPOSE, key="loan_purpose")
    with c3:
        st.subheader("Historique de crédit")
        st.number_input("Score de crédit", 300, 850, key="credit_score")
        st.number_input("Comptes ouverts", 0, 50, key="num_open_accounts")
        st.number_input("Retards de paiement", 0, 50, key="num_late_payments")
        dti = st.session_state.loan_amount / st.session_state.income
        st.metric("Ratio prêt / revenu (calculé)", f"{dti:.2f}")
    submitted = st.form_submit_button("Évaluer la demande", type="primary", use_container_width=True)

if submitted:
    payload = {k: st.session_state[k] for k in PROFILES["Profil sûr"]}
    payload["debt_to_income"] = round(st.session_state.loan_amount / st.session_state.income, 3)
    with st.spinner("Scoring en cours (le premier appel après un chargement peut prendre ~1 min)…"):
        try:
            res = api_post("/predict", payload)
        except Exception as e:
            st.error(f"Erreur de l'API : {e}")
            res = None
    if res:
        p, level = res["default_probability"], res["risk_level"]
        r1, r2, r3 = st.columns(3)
        r1.metric("Probabilité de défaut", f"{p:.1%}")
        r2.metric("Niveau de risque", {"low": "Faible", "medium": "Moyen", "high": "Élevé"}[level])
        r3.metric("Modèle utilisé", f"v{res['model_version']}")
        st.progress(min(max(p, 0.0), 1.0))
        msg = {"low": st.success, "medium": st.warning, "high": st.error}[level]
        msg({"low": "Risque faible : demande favorable.",
             "medium": "Risque moyen : étude complémentaire recommandée.",
             "high": "Risque élevé : demande défavorable."}[level])
        st.session_state.history.insert(0, {
            "heure": datetime.now().strftime("%H:%M:%S"), "revenu": payload["income"],
            "montant": payload["loan_amount"], "score": payload["credit_score"],
            "retards": payload["num_late_payments"], "probabilité": f"{p:.1%}", "risque": level,
            "modèle": f"v{res['model_version']}"})

if st.session_state.history:
    st.subheader("Historique des évaluations (session)")
    st.dataframe(pd.DataFrame(st.session_state.history), use_container_width=True, hide_index=True)

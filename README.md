# Pipeline MLOps – Risque de crédit

**Abdessamad Abouchraa** — Master Data & AI, projet de fin de module MLOps

- 📄 Rapport : [`rapport/main.pdf`](rapport/main.pdf)
- 🎬 Vidéo de démonstration (7 min) : [`demo/demo_projet_mlops.mp4`](demo/demo_projet_mlops.mp4)

## Démarrage rapide

Prérequis : Windows avec **Docker Desktop** (~8 Go de RAM alloués), PowerShell. Pour la partie Kubernetes : **minikube** et **kubectl**.

```powershell
git clone <url-du-depot>
cd <dossier-du-depot>
.\start.ps1          # crée .env depuis .env.example, vérifie/télécharge les images manquantes, démarre tout
```

Au premier lancement, le générateur publie ~150 000 demandes (≈ 25 min), puis le DAG Airflow `credit_risk_pipeline`
construit silver/gold, versionne les données (DVC), entraîne les 4 modèles et promeut le meilleur. Pour ne pas attendre
le lancement quotidien : bouton ▶ du DAG dans Airflow (http://localhost:8082, `admin` / `admin`).

## Architecture

Pipeline de bout en bout, entièrement sous Docker :

```
générateur Python ─► Kafka ─► Spark Structured Streaming (continu) ─► MinIO bronze
                                                                          │
Airflow, DAG quotidien credit_risk_pipeline :                             ▼
  bronze_to_silver ─► compact_lake ─► silver_to_gold ─► check_new_data ─┬─► dvc_snapshot ─► train_models ─► promote_model
                                                                        └─► (moins de 5 % de nouvelles données : arrêt, tâches "skipped")
                                                                          │
                     MLflow (suivi + registre, modèles stockés dans MinIO) ◄┘
                                                                          │
                                              API FastAPI  /predict  (modèle "champion")
```

| Tâche du DAG | Rôle |
|---|---|
| `bronze_to_silver` | Batch incrémental (Structured Streaming `availableNow`) : parse, valide, déduplique (MERGE), rejette les lignes invalides |
| `compact_lake` | `OPTIMIZE` (regroupe les petits fichiers du streaming) + `VACUUM` (anciennes versions > 7 jours) |
| `silver_to_gold` | Variables du modèle + `label` → table gold (contrat ML) |
| `check_new_data` | **Réentraînement conditionnel** : continue seulement si gold a grossi d'au moins `MIN_NEW_DATA_RATIO` (5 %) depuis le dernier modèle ; sinon la suite est *skipped* |
| `dvc_snapshot` | **DVC automatique** : exporte gold, `dvc add` + `dvc push` vers MinIO, commit git ; passe le hash à l'entraînement |
| `train_models` | 4 modèles (régression logistique, forêt aléatoire, GBT, XGBoost), suivis dans MLflow (tags `dvc_data_hash`, `gold_delta_version`), le meilleur est enregistré |
| `promote_model` | Alias `champion` si le nouveau modèle n'est pas moins bon que le champion de plus de `PROMOTION_TOLERANCE` (0,01 AUC) |

## Démarrer / arrêter

| Action | Commande |
|---|---|
| Tout démarrer | `.\start.ps1` (ou double-clic sur `start.cmd`) |
| Démarrer sans le générateur | `.\start.ps1 -NoGenerator` |
| Reconstruire les images | `.\start.ps1 -Rebuild` |
| Tout arrêter (données conservées) | `.\stop.ps1` (ou `stop.cmd`) |
| Tout arrêter **et effacer les données** | `.\stop.ps1 -Purge` |
| Simuler l'arrivée de nouvelles demandes | `.\generate_more.ps1 -MB 10` |
| Versionner le gold avec DVC à la main (le DAG le fait déjà) | `.\dvc_snapshot.ps1` |

`start.ps1` vérifie que Docker tourne, **télécharge uniquement les images absentes** (`scripts/ensure_images.ps1`, liste dans `scripts/images.txt`), construit les images du projet, démarre tout et attend que les services soient sains.

Prévoir ~8 Go de RAM pour Docker Desktop. Désactiver la mise en veille de Windows pendant les tests.

## Interfaces

| Service | URL | Identifiants |
|---|---|---|
| Spark (1 master, **1 seul worker**) | http://localhost:8080 | – |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| MLflow | http://localhost:5000 | – |
| Airflow | http://localhost:8082 | `admin` / `admin` |
| API (Swagger) | http://localhost:8000/docs | – |
| **Interface web (Streamlit)** | http://localhost:8501 | – |
| **Kafka UI** (topics, messages, consumers) | http://localhost:8085 | – |

Ports, identifiants, taille du worker et limites du générateur : fichier `.env`.

## Organisation des données dans MinIO

| Bucket / chemin | Contenu | Pourquoi |
|---|---|---|
| `lake/bronze/credit_applications` | Messages Kafka bruts, Delta, partitionnés par `ingest_date/ingest_hour` | Écriture rapide en continu, rejouable |
| `lake/silver/credit_applications` | Données typées, validées, dédupliquées (MERGE), partitionnées par `event_date` | Données fiables |
| `lake/silver/_rejected/...` | Lignes invalides | Traçabilité de la qualité |
| `lake/gold/credit_risk_features` | 1 ligne par demande : variables + `label` | Contrat pour le ML |
| `checkpoints/` | Checkpoints du streaming | Reprise après redémarrage |
| `mlflow/` | Artefacts MLflow (modèles) | Artifact store |
| `dvc/` | Remote DVC | Versions du jeu de données |

Silver et gold sont en **Delta Lake** : la lecture ML ne voit jamais un fichier à moitié écrit, la compaction (`OPTIMIZE`) regroupe les petits fichiers du streaming, et le time travel permet de retrouver la version exacte des données d'un entraînement (tag `gold_delta_version` dans MLflow).

## Générateur

`generator/producer.py` produit des demandes de crédit synthétiques (le label dépend des variables, plus du bruit ; ~2 % de lignes sales et ~1 % de doublons pour tester silver). Il **s'arrête après `MAX_DATA_MB` (60 Mo ≈ 150 000 demandes ≈ 147 000 lignes valides)** ; son compteur est conservé dans un volume, donc un redémarrage ne régénère pas de données. `.\stop.ps1 -Purge` le remet à zéro.

Il est lancé par `start.ps1`, **pas par Airflow** : il joue le rôle du monde extérieur (les clients qui déposent des demandes). Airflow ne fabrique pas les données, il traite celles qui arrivent et décide de réentraîner (`check_new_data`). Pour simuler de nouvelles arrivées : `.\generate_more.ps1 -MB 10`.

## Modèles

`jobs/train.py` entraîne **quatre** modèles sur gold — trois SparkML (régression logistique, forêt aléatoire, GBT) et **XGBoost** (`xgboost.spark.SparkXGBClassifier`, intégration officielle XGBoost/Spark, 2 workers sur le worker Spark unique) — avec un découpage déterministe (hash de `application_id`), journalise paramètres + AUC / AUC-PR dans MLflow et enregistre le meilleur sous `credit_risk`. Chaque run porte le tag `dvc_data_hash` : on sait avec quelles données exactes chaque modèle a été entraîné (`git checkout <commit>` + `dvc pull` les restaure).

`jobs/promote.py` donne l'alias `champion` au nouveau modèle sauf s'il est moins bon que le champion de plus de `PROMOTION_TOLERANCE`. Une tolérance plutôt qu'un « strictement meilleur » : les deux AUC sont mesurées sur des jeux de test différents (gold grandit), et un ancien modèle entraîné sur 10 fois moins de données ne doit pas rester champion à cause du bruit. La décision et sa raison sont notées sur la version (tag `promotion_decision`).

## API

```powershell
$body = '{"age":35,"income":52000,"employment_years":8,"loan_amount":15000,"loan_term_months":36,
          "interest_rate":9.5,"credit_score":690,"num_open_accounts":4,"num_late_payments":0,
          "debt_to_income":0.29,"home_ownership":"RENT","loan_purpose":"car"}'
Invoke-RestMethod http://localhost:8000/predict -Method Post -ContentType 'application/json' -Body $body
# -> default_probability, prediction, risk_level (low / medium / high), model_version
```
`POST /reload` recharge le champion après une nouvelle promotion. Le premier appel est lent (démarrage de Spark dans l'API).

## Interface web

`ui/app.py` (Streamlit, http://localhost:8501) appelle l'API : formulaire de demande de prêt (client, prêt, historique de crédit), trois profils d'exemple, probabilité de défaut, niveau de risque et version du modèle utilisé, historique des évaluations de la session, bouton « Recharger le modèle champion ».

## Déploiement Kubernetes (minikube)

La partie **serving** (API + interface) est déployée dans minikube ; l'entraînement reste sous Docker Compose. Le modèle champion est exporté de MLflow et **intégré dans l'image** `credit-risk-serving:v<version>` : dans le cluster, l'API ne dépend ni de MLflow ni de MinIO, et chaque nouveau champion donne une nouvelle image versionnée.

| Action | Commande |
|---|---|
| Déployer le champion actuel | `.\k8s\deploy.ps1 -StopCompose` (Compose doit tourner au départ ; il est arrêté après l'export pour libérer la mémoire) |
| Ouvrir l'UI / l'API depuis Windows | `.\k8s\port-forward.ps1` → UI http://localhost:8601, API http://localhost:8100/docs |
| Supprimer le déploiement | `.\k8s\undeploy.ps1` (`-StopMinikube` arrête aussi minikube) |
| Revenir à la stack d'entraînement | `.\k8s\undeploy.ps1 -StopMinikube` puis `.\start.ps1` |

`k8s/credit-risk.yaml` : namespace `credit-risk`, ConfigMap, Deployment + Service de l'API (sonde *readiness* `/ready` = 200 seulement quand le modèle est chargé, *liveness* `/health`, limites CPU/RAM), Deployment + Service NodePort de l'UI (qui joint l'API par le DNS du cluster `http://credit-risk-api:8000`). L'image est chargée dans minikube (`imagePullPolicy: Never`) seulement si elle n'y est pas déjà.

Avec ~8 Go pour Docker, Compose et minikube ne tiennent pas ensemble : d'où `-StopCompose`. Les données restent dans les volumes.

## Arborescence

```
docker-compose.yml  .env.example  start.ps1  stop.ps1  generate_more.ps1  dvc_snapshot.ps1
docker/spark/Dockerfile      image mlops-spark (Spark + jars S3A/Kafka/Delta + MLflow + DVC + code)
docker/airflow/Dockerfile    Airflow + provider Docker
docker/postgres/init.sql     bases mlflow et airflow
generator/producer.py
jobs/                        jobs Spark, entraînement, promotion, export DVC
airflow/dags/credit_risk_pipeline.py
api/main.py                  API FastAPI (modèle MLflow en Compose, modèle intégré à l'image en K8s)
ui/app.py                    interface Streamlit
k8s/                         déploiement Kubernetes (manifestes, Dockerfile.serving, deploy/undeploy/port-forward)
scripts/                     vérification des images avant téléchargement (images.txt)
data/gold_snapshot.dvc       version DVC du jeu gold (données dans MinIO, bucket dvc)
rapport/main.pdf             rapport du projet
demo/demo_projet_mlops.mp4   vidéo de démonstration
```

Après une modification du code dans `jobs/`, `generator/` ou `api/` : `docker compose build spark-master` puis `.\start.ps1`.

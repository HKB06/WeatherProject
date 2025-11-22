# ☁️ WeatherProject - Big Data Pipeline

Pipeline d'analyse météorologique Big Data avec Apache Spark, PostgreSQL et Flask.

---

## 📋 Contexte & Choix du Dataset

Ce projet s'appuie sur des **données météorologiques réelles** de la Côte d'Azur, couvrant la période **2023-2025** (plus de 1000 jours).

Chaque enregistrement contient :
- **Date** de l'observation
- **Températures** (minimale, maximale, moyenne)
- **Humidité relative** moyenne
- **Précipitations** cumulées
- **Vitesse du vent** maximale
- **Localisation** (5 villes : Nice, Cannes, Monaco, Antibes, Menton)

### Pourquoi ce dataset ?

- **Dimension temporelle** : idéal pour analyser les tendances saisonnières et l'évolution du climat
- **Dimension géographique** : comparaison entre 5 villes de la Côte d'Azur
- **Données publiques** : provenant d'Open-Meteo Archive API (source officielle)
- **Pertinence métier** : applications en tourisme, agriculture, prévention des risques

---

## 🎯 Questions Analytiques

Le projet répond à plusieurs questions d'analyse météorologique :

### 1. Comment évoluent les températures dans le temps ?
- Quelle est la **température moyenne** sur 3 ans ?
- Observe-t-on une **tendance** à la hausse ou à la baisse ?
- Quelles sont les **variations saisonnières** ?

### 2. Quelles sont les périodes les plus extrêmes ?
- Quel **mois** enregistre les températures les plus élevées/basses ?
- Quelle **saison** est la plus pluvieuse ?
- Quelle **année** a été la plus chaude ?

### 3. Existe-t-il des différences entre les villes de la Côte d'Azur ?
- Quelle ville a la **température moyenne** la plus élevée ?
- Quelle ville reçoit le **plus de précipitations** ?
- Les **tendances** sont-elles similaires ou différentes entre les villes ?

### 4. Comment se répartissent les températures ?
- Quelle est la **distribution** des températures journalières ?
- Combien de jours dépassent **30°C** (canicule) ?
- Combien de jours sont en dessous de **5°C** (froid) ?

### 5. Quels sont les patterns saisonniers ?
- Quelle est la **température moyenne par saison** ?
- La **variabilité** est-elle plus forte en été ou en hiver ?
- Observe-t-on des **anomalies** saisonnières ?

---

## 🏗️ Architecture du Projet

L'architecture suit une logique de **DataLake en 3 couches** : Ingestion, Persistance, Insight.

### 1. Ingestion

**Source de données** : Open-Meteo Archive API (gratuite, sans clé)

- **Type 1** : API REST (JSON) - données temps réel/historiques
- **Type 2** : Conversion CSV - données structurées pour Spark

**Processus** :
- Appel API pour 5 villes (Nice, Cannes, Monaco, Antibes, Menton)
- Période : 2023-01-01 → 2025-11-22 (aujourd'hui)
- Sauvegarde brute : `data/raw/*.json` et `data/raw/*.csv`
- Résilience : checkpoints, retry, logs

### 2. Persistance (DataLake)

Les données sont organisées en **3 zones** :

#### Zone RAW (données brutes)
- **Localisation** : `data/raw/`
- **Formats** : JSON (API) + CSV (conversion)
- **Contenu** : Données brutes telles que reçues de l'API
- **Conservation** : Toutes les données sources sont conservées

#### Zone CURATED (données nettoyées)
- **Traitement Spark** :
  - Nettoyage des valeurs manquantes
  - Validation des données (températures cohérentes, dates valides)
  - Conversion des types (dates, nombres)
  - Ajout de colonnes dérivées (année, mois, saison)

#### Zone ANALYTICS (données agrégées)
- **Localisation** : `data/processed/`
- **Format** : Parquet (optimisé, compressé)
- **Datasets générés** :
  - `daily/daily.parquet` - Agrégations journalières (1057 jours)
  - `monthly/monthly.parquet` - Agrégations mensuelles (35 mois)
  - `seasonal/seasonal.parquet` - Agrégations saisonnières (12 saisons)

#### Métadonnées (PostgreSQL)
- Table `ingestion_metadata` : traçabilité des ingestions
- Table `processing_metadata` : statistiques de traitement
- Séparation stricte données/métadonnées

### 3. Insight (Dashboard & Visualisation)

**Infrastructure Docker** :
- **PostgreSQL** : Métadonnées et traçabilité
- **Spark Master** : Coordination du traitement distribué
- **Spark Worker** : Exécution des tâches Spark
- **Dashboard Flask** : Application web de visualisation

**Fonctionnalités du Dashboard** :
- Statistiques globales (température, précipitations, humidité)
- Graphique d'évolution temporelle avec **filtres dynamiques** (30j, 1an, 2ans, 3ans)
- Moyennes mensuelles et saisonnières
- Heatmap des températures par mois
- Distribution des températures
- Analyse des précipitations
- Export de données

---

## 🔄 Pipeline End-to-End

Le script `run_pipeline.py` orchestre la pipeline complète :

### Étape 1 : Ingestion des Données
```
Appel API Open-Meteo Archive
  ↓
Sauvegarde JSON (data/raw/)
  ↓
Conversion CSV pour Spark
  ↓
Sauvegarde métadonnées PostgreSQL
```

### Étape 2 : Traitement Spark
```
Lecture CSV brut
  ↓
Nettoyage & Validation (5285 → 5285 records)
  ↓
Enrichissement (colonnes dérivées)
  ↓
Agrégations :
  - Journalières (1057 jours)
  - Mensuelles (35 mois)
  - Saisonnières (12 saisons)
  ↓
Calcul des tendances
```

### Étape 3 : Persistance
```
Conversion Spark → Pandas
  ↓
Sauvegarde Parquet (compression Snappy)
  ↓
Sauvegarde métadonnées PostgreSQL
```

### Étape 4 : Visualisation
```
Dashboard Flask lit les Parquet
  ↓
API REST pour les données
  ↓
Graphiques interactifs (Plotly.js, Chart.js)
```

**Durée totale** : ~35-40 secondes

---

## 🚀 Prérequis

- **Git**
- **Docker** et **Docker Compose**
- **Python** 3.11+ (pour exécution locale du pipeline)
- **Connexion Internet** (téléchargement des données)

---

## 📥 Installation & Lancement

### 1. Cloner le projet

```bash
git clone https://github.com/HKB06/WeatherProject.git
cd WeatherProject
```

### 2. Créer le fichier `.env`

```bash
# Windows
copy .env.example .env

# Linux/Mac
cp .env.example .env
```

Contenu minimal du `.env` :
```env
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=weather_metadata
POSTGRES_USER=weather_admin
POSTGRES_PASSWORD=weather_pass_2025
```

### 3. Créer l'environnement virtuel Python (optionnel, pour exécution locale)

**Windows** :
```bash
python -m venv env
env\Scripts\activate
pip install -r requirements.txt
```

**Linux/Mac** :
```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

### 4. Démarrer l'infrastructure Docker

```bash
docker-compose up -d
```

**Services démarrés** :
- PostgreSQL (port 5432)
- Spark Master (port 8080)
- Spark Worker
- Dashboard Flask (port 5000)

### 5. Lancer la pipeline

```bash
python run_pipeline.py
```

**Sortie attendue** :
```
PHASE 1: INGESTION DES DONNÉES RÉELLES
  ✓ 5 villes ingérées (Nice, Cannes, Monaco, Antibes, Menton)
  ✓ 5285 enregistrements bruts

PHASE 2: TRAITEMENT SPARK
  ✓ 1057 jours agrégés
  ✓ 35 mois agrégés
  ✓ 12 saisons agrégées

PHASE 3: PERSISTANCE DES DONNÉES
  ✓ Parquet sauvegardés (0.07 MB)
  ✓ Métadonnées PostgreSQL OK

PIPELINE TERMINÉ AVEC SUCCÈS (35.8s)
```

### 6. Accéder au dashboard

**Dashboard** : http://localhost:5000

**Spark UI** : http://localhost:8080

---

## 📊 Utilisation du Dashboard

Le dashboard Flask propose plusieurs visualisations interactives :

### 1. Vue d'Ensemble
- **Température moyenne** : 17.0°C
- **Précipitations totales** : 12 910 mm
- **Humidité moyenne** : 69.5%
- **Période d'analyse** : 1057 jours

### 2. Évolution de la Température
- **Graphique interactif** (Plotly.js) avec 3 courbes :
  - Température moyenne (ligne bleue)
  - Température maximale (ligne rouge pointillée)
  - Température minimale (ligne cyan pointillée)
- **Filtres dynamiques** :
  - 30 derniers jours
  - 90 derniers jours
  - 1 an
  - 2 ans
  - 3 ans (complet)
- **Interaction** : Zoom, pan, hover pour détails

### 3. Analyses Temporelles
- **Moyennes mensuelles** : température et précipitations par mois
- **Comparaison saisonnière** : graphique radar par saison
- **Heatmap** : températures par mois (2023-2025)

### 4. Analyses Avancées
- **Précipitations mensuelles** : bar chart interactif
- **Distribution des températures** : histogramme (Chart.js)
- Identification des **anomalies climatiques**

### 5. Gestion des Données
- **Filtres** :
  - Date de début
  - Date de fin
  - Limite de résultats
- **Table interactive** : tri, recherche, pagination
- **Export CSV** : téléchargement des données filtrées

---

## 🛠️ Technologies & Stack Technique

### Big Data
- **Apache Spark 3.3.0** - Traitement distribué
- **PySpark** - API Python pour Spark
- **Parquet** - Format de stockage optimisé

### Base de Données
- **PostgreSQL 16** - Métadonnées et traçabilité
- **psycopg2** - Driver Python pour PostgreSQL

### Web & Visualisation
- **Flask 3.0** - Framework web Python
- **Plotly.js** - Graphiques interactifs
- **Chart.js** - Visualisations complémentaires

### Orchestration
- **Docker** - Containerisation
- **Docker Compose** - Orchestration multi-conteneurs

### Ingestion
- **Open-Meteo Archive API** - Données météorologiques officielles
- **Requests** - Client HTTP Python

---

## ⚙️ Configuration Avancée

### Modifier les villes analysées

Éditer `config/config.yaml` :

```yaml
data_sources:
  api:
    cities:
      - name: "Nice"
        latitude: 43.7102
        longitude: 7.2620
      # Ajouter d'autres villes ici
```

### Changer la période d'analyse

Dans `src/ingestion/api_ingestion.py` :

```python
start_date = datetime(2023, 1, 1).date()  # Modifier l'année
end_date = datetime.now().date()
```

### Ajuster les ressources Spark

Dans `docker-compose.yml` :

```yaml
spark-worker:
  environment:
    - SPARK_WORKER_CORES=4      # Nombre de cores
    - SPARK_WORKER_MEMORY=4G    # Mémoire allouée
```

---

## 🔍 Conformité avec le Cahier des Charges

### Exigences TP → Implémentation

| Exigence | Implémentation |
|----------|----------------|
| **2 sources de données différentes** | ✅ API REST (JSON) + CSV historiques |
| **Ingestion résiliente** | ✅ Checkpoints, retry, logs détaillés |
| **Données brutes conservées** | ✅ `data/raw/` (JSON + CSV) |
| **Métadonnées séparées** | ✅ PostgreSQL (tables dédiées) |
| **ETL complet** | ✅ Spark (Extract, Transform, Load) |
| **Indexation** | ✅ Parquet avec compression Snappy |
| **Dashboard interactif** | ✅ Flask + Plotly.js (filtres dynamiques) |
| **Framework Big Data** | ✅ Apache Spark distribué |
| **Insights significatifs** | ✅ Tendances, saisonnalité, anomalies |
| **Architecture DataLake** | ✅ 3 couches (RAW, CURATED, ANALYTICS) |

---

## 📉 Limites & Pistes d'Amélioration

### Limites actuelles

1. **Géographie limitée** : Seulement la Côte d'Azur (pas de comparaison nationale/européenne)
2. **Traitement batch** : Pas de streaming temps réel
3. **Modèle statistique simple** : Pas de prédiction météo (Machine Learning)
4. **API gratuite** : Limites de fréquence et de granularité
5. **Pas de détection d'anomalies avancée** : Approche purement descriptive

### Améliorations possibles

1. **Étendre géographiquement** :
   - Ajouter d'autres régions françaises
   - Comparaison inter-régions

2. **Streaming temps réel** :
   - Intégration Kafka + Spark Streaming
   - Mise à jour automatique du dashboard

3. **Machine Learning** :
   - Prédiction des températures (LSTM, Prophet)
   - Détection d'anomalies climatiques (Isolation Forest)
   - Clustering de patterns météo

4. **Enrichissement des données** :
   - Ajout de la qualité de l'air
   - Données satellite
   - Événements météo extrêmes

5. **Optimisations techniques** :
   - Partitionnement Parquet par année/mois
   - Cache Redis pour le dashboard
   - API REST pour intégration externe

6. **Analyses avancées** :
   - Corrélation avec données touristiques
   - Impact sur l'agriculture locale
   - Analyse prédictive des canicules

---

## 🛑 Arrêt du Projet

```bash
# Arrêter tous les conteneurs
docker-compose down

# Supprimer les volumes (attention : perte des métadonnées)
docker-compose down -v
```

---

## 👥 Auteurs

**Hugo K.** 

---

## 🔗 Liens Utiles

- [Open-Meteo Archive API](https://open-meteo.com/en/docs/historical-weather-api)
- [Apache Spark Documentation](https://spark.apache.org/docs/3.3.0/)
- [Flask Documentation](https://flask.palletsprojects.com/)
- [Plotly.js Documentation](https://plotly.com/javascript/)

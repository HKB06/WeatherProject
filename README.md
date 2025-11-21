# ☁️ WeatherProject - Big Data Pipeline

Pipeline d'analyse météorologique Big Data avec Apache Spark, PostgreSQL et Flask.

## 🚀 Technologies

- **Apache Spark 3.3.0** - Traitement distribué
- **PostgreSQL 16** - Métadonnées
- **Flask 3.0** - Dashboard web
- **Docker** - Containerisation complète
- **NOAA API** - Données météo réelles

## 🔧 Démarrage Rapide

### 1. Créer le fichier .env

```env
NOAA_API_TOKEN=votre_token_ici
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=weather_metadata
POSTGRES_USER=weather_admin
POSTGRES_PASSWORD=weather_pass_2024
```

### 2. Lancer les services Docker

```bash
docker-compose up -d
```

### 3. Lancer le pipeline

```bash
python run_pipeline.py
```

### 4. Accéder au dashboard

```
http://localhost:5000
```

## 📊 Services

- **Dashboard**: http://localhost:5000
- **Spark UI**: http://localhost:8080  
- **PostgreSQL**: localhost:5432

## 📂 Structure

```
WeatherProject/
├── src/
│   ├── ingestion/      # Données NOAA/API
│   ├── processing/     # Spark processing
│   ├── persistence/    # Sauvegarde Parquet
│   └── utils/         # Logger, checkpoints
├── dashboard/         # Interface Flask
├── data/
│   ├── raw/          # Données brutes
│   └── processed/    # Parquet
├── config/           # Configuration
├── docker-compose.yml
└── run_pipeline.py
```

## 🎯 Pipeline

1. **Ingestion** - Téléchargement données NOAA + Open-Meteo
2. **Processing** - Spark : nettoyage, agrégations, tendances
3. **Persistance** - Sauvegarde Parquet + métadonnées PostgreSQL
4. **Dashboard** - Visualisation interactive

## 🛑 Arrêter

```bash
docker-compose down
```

## 📝 Projet IPSSI - Big Data

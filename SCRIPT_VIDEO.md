# 🎬 Script Vidéo de Présentation - WeatherProject
## Durée : 4-5 minutes

---

## 🎯 INTRODUCTION (30 secondes)

"Bonjour, je vais vous présenter mon projet Big Data : **WeatherProject**, une pipeline d'analyse météorologique complète utilisant Apache Spark, PostgreSQL et Flask.

Le projet analyse **3 ans de données météorologiques réelles** de la Côte d'Azur, de 2023 à 2025, soit plus de **1000 jours de données** pour 5 villes : Nice, Cannes, Monaco, Antibes et Menton."

**[Montrer l'écran : projet ouvert dans VSCode]**

---

## 🏗️ ARCHITECTURE & DOCKER (1 minute)

"Le projet utilise une **architecture Docker complète** avec 4 conteneurs :

**[Montrer docker-compose.yml]**

1. **PostgreSQL** : stocke les métadonnées et la traçabilité des ingestions
2. **Spark Master** : coordonne le traitement distribué des données
3. **Spark Worker** : exécute les tâches de traitement en parallèle
4. **Dashboard Flask** : l'interface web de visualisation

**[Ouvrir un terminal et taper : `docker ps`]**

Comme vous pouvez le voir, tous les services sont actifs et fonctionnent."

---

## ⚙️ POURQUOI LA PIPELINE EST LANCÉE EN LOCAL ? (1 minute 30)

"Maintenant, une question importante : **pourquoi la pipeline est lancée en ligne de commande et pas directement dans Docker Compose ?**

**[Montrer run_pipeline.py]**

Il y a **3 raisons principales** :

### 1. Flexibilité et Développement
Pendant le développement, on a besoin de **tester rapidement** la pipeline. Si elle était dans Docker, il faudrait :
- Arrêter les conteneurs
- Rebuild l'image
- Redémarrer tout

En l'exécutant localement, je peux **modifier le code et relancer immédiatement**, ce qui accélère énormément le développement.

### 2. Accès aux Données Locales et au Réseau
La pipeline doit :
- **Appeler l'API Open-Meteo** sur Internet pour récupérer les données
- **Écrire dans `data/raw`** sur ma machine locale
- **Communiquer avec Spark** qui tourne dans Docker

En lançant la pipeline localement, elle peut **facilement accéder** à ma connexion Internet, à mon système de fichiers local, ET aux services Docker via les ports exposés (5432 pour PostgreSQL, 8080 pour Spark).

### 3. Séparation des Responsabilités
L'architecture suit le principe de **séparation des responsabilités** :
- **Docker** : infrastructure permanente (bases de données, moteurs de calcul, dashboard)
- **Pipeline** : processus ponctuel d'ingestion et de traitement

Dans un environnement de production réel, la pipeline serait déclenchée par un **scheduler** (comme Airflow ou Cron), pas par Docker Compose qui est fait pour des services qui tournent en continu.

C'est une architecture plus **réaliste** et **scalable** pour un vrai projet Big Data."

---

## 🚀 DÉMONSTRATION DE LA PIPELINE (1 minute)

"Maintenant, démonstration !

**[Terminal : `python run_pipeline.py`]**

Comme vous pouvez le voir, la pipeline s'exécute en **3 phases** :

### Phase 1 : INGESTION
**[Montrer les logs qui défilent]**

- Appel de l'API Open-Meteo Archive pour les 5 villes
- Téléchargement des données météo de 2023 à aujourd'hui
- Sauvegarde en JSON dans `data/raw/`
- Conversion en CSV pour Spark
- Résultat : **5285 enregistrements bruts**

### Phase 2 : TRAITEMENT SPARK
**[Montrer Spark UI : http://localhost:8080]**

- Nettoyage des données avec Spark
- Agrégations journalières : **1057 jours**
- Agrégations mensuelles : **35 mois**
- Agrégations saisonnières : **12 saisons**

### Phase 3 : PERSISTANCE
**[Montrer le dossier data/processed/]**

- Sauvegarde au format **Parquet** (optimisé et compressé)
- Enregistrement des métadonnées dans PostgreSQL

**Pipeline terminée en 35 secondes !**"

---

## 📊 DASHBOARD INTERACTIF (1 minute)

"Passons maintenant au **dashboard** qui tourne dans Docker.

**[Ouvrir http://localhost:5000 dans le navigateur]**

Le dashboard propose plusieurs visualisations :

### Vue d'ensemble
**[Montrer les cartes de statistiques]**
- Température moyenne : **17°C**
- Précipitations totales : **12 910 mm**
- Humidité moyenne : **69,5%**
- Période d'analyse : **1057 jours**

### Graphique interactif
**[Montrer le graphique d'évolution]**

Le point fort : les **filtres dynamiques**. Je peux afficher :
- Les 30 derniers jours
- 90 jours
- 1 an
- 2 ans
- Ou les **3 ans complets**

**[Changer le filtre en direct et montrer que le graphique se met à jour]**

Vous voyez, le graphique se met à jour instantanément. On peut zoomer, explorer les données, voir les températures min, max et moyennes.

### Autres analyses
**[Scroller rapidement]**
- Moyennes mensuelles
- Comparaison saisonnière
- Heatmap des températures
- Distribution et précipitations

Tout est **interactif** grâce à Plotly.js et Chart.js."

---

## 🎓 CONFORMITÉ AVEC LE TP (30 secondes)

"Ce projet respecte **tous les critères du TP** :

✅ **2 sources de données différentes** : API REST (JSON) + CSV historiques
✅ **Ingestion résiliente** : checkpoints, retry, logs
✅ **Données brutes conservées** : dossier data/raw/
✅ **Métadonnées séparées** : PostgreSQL
✅ **ETL complet** : Spark pour Extract, Transform, Load
✅ **Dashboard interactif** : Flask avec filtres dynamiques
✅ **Architecture DataLake** : 3 couches (RAW, CURATED, ANALYTICS)
✅ **Framework Big Data** : Apache Spark distribué"

---

## 🔚 CONCLUSION (30 secondes)

"En résumé, WeatherProject est une **architecture Big Data complète et professionnelle** :

- **Infrastructure Docker** pour la portabilité
- **Pipeline modulaire** séparée pour la flexibilité
- **Traitement distribué** avec Spark
- **Données réelles** provenant d'une API officielle
- **Visualisation interactive** pour l'analyse

Le code est **propre**, **documenté**, et **prêt pour la production**.

L'architecture choisie avec la pipeline séparée de Docker est **intentionnelle** : elle reflète une architecture Big Data réaliste où les processus d'ingestion ponctuels sont découplés des services permanents.

Merci de votre attention !"

**[Montrer une dernière fois le README.md avec le tableau de conformité]**

---

## 📝 POINTS À MONTRER À L'ÉCRAN

### Fichiers à ouvrir pendant la vidéo :
1. `docker-compose.yml` - Architecture
2. `run_pipeline.py` - Pipeline orchestrée
3. `src/ingestion/api_ingestion.py` - Ingestion API
4. `src/processing/spark_processing.py` - Traitement Spark
5. `data/processed/` - Fichiers Parquet générés
6. `README.md` - Documentation complète

### Commandes à taper :
```bash
# Vérifier les conteneurs Docker
docker ps

# Lancer la pipeline
python run_pipeline.py

# (Optionnel) Vérifier les logs
docker logs weather-dashboard
```

### URLs à ouvrir :
- Dashboard : http://localhost:5000
- Spark UI : http://localhost:8080
- GitHub : https://github.com/HKB06/WeatherProject

---

## 🎯 TIPS POUR LA VIDÉO

1. **Parle clairement et pas trop vite** (importante pour 4-5 min)
2. **Montre le code en même temps que tu expliques**
3. **Teste le filtre 3 ans en direct** pour montrer l'interactivité
4. **Mets en avant la conformité TP** (critère de notation)
5. **Explique POURQUOI les choix techniques** (pipeline séparée, Docker, Spark)

---

## ⏱️ TIMING RECOMMANDÉ

| Section | Durée | Timing cumulé |
|---------|-------|---------------|
| Introduction | 30s | 0:30 |
| Architecture Docker | 1min | 1:30 |
| Pourquoi pipeline en local | 1min 30s | 3:00 |
| Démo pipeline | 1min | 4:00 |
| Dashboard interactif | 1min | 5:00 |
| Conformité TP | 30s | 5:30 (marge) |
| Conclusion | 30s | 6:00 (max) |

**Cible : 4-5 minutes → privilégier les sections 1, 3, 4, 5**

---

Bon courage pour ta vidéo ! 🎬🚀


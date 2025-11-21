"""
Module de traitement des données météo avec Apache Spark
Effectue le nettoyage, les transformations et les agrégations
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
import yaml

# Configuration Spark
os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Ajouter le répertoire parent au path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import get_logger

logger = get_logger(__name__)


class WeatherSparkProcessor:
    """Classe pour le traitement Spark des données météo"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.paths = self.config.get('paths', {})
        self.processing_config = self.config.get('processing', {})
        self.validation_rules = self.processing_config.get('validation', {})
        
        logger.info("WeatherSparkProcessor initialisé")
    
    def _load_config(self, config_path: str) -> dict:
        """Charge la configuration"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Erreur lors du chargement de la config: {e}")
            raise
    
    def _create_spark_session(self) -> SparkSession:
        """Crée une session Spark"""
        try:
            spark_config = self.config.get('spark', {})
            
            # Récupérer master depuis env ou config
            master = os.getenv('SPARK_MASTER', spark_config.get('master', 'local[*]'))
            
            builder = SparkSession.builder \
                .appName(spark_config.get('app_name', 'WeatherProject')) \
                .master(master)
            
            # Ajouter les configurations
            for key, value in spark_config.get('config', {}).items():
                builder = builder.config(key, value)
            
            spark = builder.getOrCreate()
            
            # Définir le niveau de log
            spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"Spark session créée: {master}")
            logger.info(f"Spark version: {spark.version}")
            
            return spark
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de la session Spark: {e}")
            raise
    
    def read_raw_csv(self, csv_path: str) -> DataFrame:
        """
        Lit un fichier CSV brut
        
        Args:
            csv_path: Chemin vers le fichier CSV
        
        Returns:
            DataFrame Spark
        """
        logger.info(f"Lecture du CSV: {csv_path}")
        
        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(csv_path)
            
            record_count = df.count()
            logger.info(f"CSV chargé: {record_count} enregistrements, {len(df.columns)} colonnes")
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de la lecture du CSV: {e}")
            raise
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Nettoie les données selon les règles de validation
        
        Args:
            df: DataFrame source
        
        Returns:
            DataFrame nettoyé
        """
        logger.info("Nettoyage des données...")
        
        initial_count = df.count()
        
        # Supprimer les doublons
        df = df.dropDuplicates()
        
        # Supprimer les lignes avec DATE null
        df = df.filter(F.col("DATE").isNotNull())
        
        # Convertir DATE en type date
        df = df.withColumn("DATE", F.to_date(F.col("DATE")))
        
        # Validation des températures
        if "TAVG" in df.columns:
            temp_min = self.validation_rules.get('temperature_min', -60)
            temp_max = self.validation_rules.get('temperature_max', 60)
            df = df.filter(
                F.col("TAVG").isNull() | 
                ((F.col("TAVG") >= temp_min) & (F.col("TAVG") <= temp_max))
            )
        
        # Validation de l'humidité
        if "RHUM" in df.columns:
            humidity_min = self.validation_rules.get('humidity_min', 0)
            humidity_max = self.validation_rules.get('humidity_max', 100)
            df = df.filter(
                F.col("RHUM").isNull() |
                ((F.col("RHUM") >= humidity_min) & (F.col("RHUM") <= humidity_max))
            )
        
        # Validation des précipitations
        if "PRCP" in df.columns:
            prcp_min = self.validation_rules.get('precipitation_min', 0)
            df = df.filter(F.col("PRCP").isNull() | (F.col("PRCP") >= prcp_min))
        
        # Validation de la vitesse du vent
        if "AWND" in df.columns:
            wind_min = self.validation_rules.get('wind_speed_min', 0)
            df = df.filter(F.col("AWND").isNull() | (F.col("AWND") >= wind_min))
        
        final_count = df.count()
        removed_count = initial_count - final_count
        retention_rate = (final_count / initial_count * 100) if initial_count > 0 else 0
        
        logger.info(f"Nettoyage terminé: {final_count}/{initial_count} enregistrements conservés ({retention_rate:.1f}%)")
        logger.info(f"Enregistrements supprimés: {removed_count}")
        
        return df
    
    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        """
        Ajoute des colonnes dérivées pour l'analyse
        
        Args:
            df: DataFrame source
        
        Returns:
            DataFrame enrichi
        """
        logger.info("Ajout de colonnes dérivées...")
        
        # Extraire année, mois, jour
        df = df.withColumn("YEAR", F.year("DATE"))
        df = df.withColumn("MONTH", F.month("DATE"))
        df = df.withColumn("DAY", F.dayofmonth("DATE"))
        df = df.withColumn("DAY_OF_WEEK", F.dayofweek("DATE"))
        df = df.withColumn("WEEK_OF_YEAR", F.weekofyear("DATE"))
        
        # Saison (basée sur le mois)
        df = df.withColumn("SEASON", 
            F.when((F.col("MONTH") >= 3) & (F.col("MONTH") <= 5), "Spring")
            .when((F.col("MONTH") >= 6) & (F.col("MONTH") <= 8), "Summer")
            .when((F.col("MONTH") >= 9) & (F.col("MONTH") <= 11), "Fall")
            .otherwise("Winter")
        )
        
        # Calculer l'amplitude thermique si TMAX et TMIN existent
        if "TMAX" in df.columns and "TMIN" in df.columns:
            df = df.withColumn("TEMP_RANGE", F.col("TMAX") - F.col("TMIN"))
        
        # Classification de la température
        if "TAVG" in df.columns:
            df = df.withColumn("TEMP_CATEGORY",
                F.when(F.col("TAVG") < 0, "Freezing")
                .when((F.col("TAVG") >= 0) & (F.col("TAVG") < 10), "Cold")
                .when((F.col("TAVG") >= 10) & (F.col("TAVG") < 20), "Mild")
                .when((F.col("TAVG") >= 20) & (F.col("TAVG") < 30), "Warm")
                .otherwise("Hot")
            )
        
        # Classification des précipitations
        if "PRCP" in df.columns:
            df = df.withColumn("RAIN_CATEGORY",
                F.when(F.col("PRCP") == 0, "No Rain")
                .when((F.col("PRCP") > 0) & (F.col("PRCP") <= 2), "Light")
                .when((F.col("PRCP") > 2) & (F.col("PRCP") <= 10), "Moderate")
                .when((F.col("PRCP") > 10) & (F.col("PRCP") <= 50), "Heavy")
                .otherwise("Extreme")
            )
        
        logger.info("Colonnes dérivées ajoutées")
        
        return df
    
    def calculate_daily_aggregations(self, df: DataFrame) -> DataFrame:
        """
        Calcule les agrégations journalières
        
        Args:
            df: DataFrame source
        
        Returns:
            DataFrame avec agrégations quotidiennes
        """
        logger.info("Calcul des agrégations journalières...")
        
        agg_exprs = [
            F.first("STATION").alias("STATION"),
            F.first("STATION_NAME").alias("STATION_NAME"),
            F.first("YEAR").alias("YEAR"),
            F.first("MONTH").alias("MONTH"),
            F.first("DAY").alias("DAY"),
            F.first("SEASON").alias("SEASON")
        ]
        
        # Agrégations de température
        if "TAVG" in df.columns:
            agg_exprs.extend([
                F.avg("TAVG").alias("TEMP_AVG"),
                F.min("TAVG").alias("TEMP_MIN"),
                F.max("TAVG").alias("TEMP_MAX"),
                F.stddev("TAVG").alias("TEMP_STD")
            ])
        
        # Agrégations d'humidité
        if "RHUM" in df.columns:
            agg_exprs.extend([
                F.avg("RHUM").alias("HUMIDITY_AVG"),
                F.min("RHUM").alias("HUMIDITY_MIN"),
                F.max("RHUM").alias("HUMIDITY_MAX")
            ])
        
        # Agrégations de précipitations
        if "PRCP" in df.columns:
            agg_exprs.extend([
                F.sum("PRCP").alias("PRECIPITATION_TOTAL"),
                F.avg("PRCP").alias("PRECIPITATION_AVG"),
                F.max("PRCP").alias("PRECIPITATION_MAX")
            ])
        
        # Agrégations de vent
        if "AWND" in df.columns:
            agg_exprs.extend([
                F.avg("AWND").alias("WIND_SPEED_AVG"),
                F.max("AWND").alias("WIND_SPEED_MAX")
            ])
        
        agg_exprs.append(F.count("*").alias("RECORDS_COUNT"))
        
        daily_agg = df.groupBy("DATE").agg(*agg_exprs)
        
        count = daily_agg.count()
        logger.info(f"Agrégations journalières calculées: {count} jours")
        
        return daily_agg
    
    def calculate_monthly_aggregations(self, df: DataFrame) -> DataFrame:
        """
        Calcule les agrégations mensuelles
        
        Args:
            df: DataFrame source
        
        Returns:
            DataFrame avec agrégations mensuelles
        """
        logger.info("Calcul des agrégations mensuelles...")
        
        agg_exprs = [
            F.first("STATION").alias("STATION"),
            F.first("STATION_NAME").alias("STATION_NAME")
        ]
        
        if "TAVG" in df.columns:
            agg_exprs.extend([
                F.avg("TAVG").alias("TEMP_AVG"),
                F.min("TAVG").alias("TEMP_MIN"),
                F.max("TAVG").alias("TEMP_MAX")
            ])
        
        if "RHUM" in df.columns:
            agg_exprs.append(F.avg("RHUM").alias("HUMIDITY_AVG"))
        
        if "PRCP" in df.columns:
            agg_exprs.extend([
                F.sum("PRCP").alias("PRECIPITATION_TOTAL"),
                F.avg("PRCP").alias("PRECIPITATION_AVG")
            ])
        
        if "AWND" in df.columns:
            agg_exprs.append(F.avg("AWND").alias("WIND_SPEED_AVG"))
        
        agg_exprs.append(F.count("*").alias("DAYS_COUNT"))
        
        monthly_agg = df.groupBy("YEAR", "MONTH").agg(*agg_exprs)
        
        count = monthly_agg.count()
        logger.info(f"Agrégations mensuelles calculées: {count} mois")
        
        return monthly_agg
    
    def calculate_seasonal_aggregations(self, df: DataFrame) -> DataFrame:
        """
        Calcule les agrégations saisonnières
        
        Args:
            df: DataFrame source
        
        Returns:
            DataFrame avec agrégations saisonnières
        """
        logger.info("Calcul des agrégations saisonnières...")
        
        agg_exprs = [
            F.first("STATION").alias("STATION"),
            F.first("STATION_NAME").alias("STATION_NAME")
        ]
        
        if "TAVG" in df.columns:
            agg_exprs.extend([
                F.avg("TAVG").alias("TEMP_AVG"),
                F.min("TAVG").alias("TEMP_MIN"),
                F.max("TAVG").alias("TEMP_MAX")
            ])
        
        if "PRCP" in df.columns:
            agg_exprs.append(F.sum("PRCP").alias("PRECIPITATION_TOTAL"))
        
        agg_exprs.append(F.count("*").alias("DAYS_COUNT"))
        
        seasonal_agg = df.groupBy("YEAR", "SEASON").agg(*agg_exprs)
        
        count = seasonal_agg.count()
        logger.info(f"Agrégations saisonnières calculées: {count} saisons")
        
        return seasonal_agg
    
    def calculate_trends(self, df: DataFrame) -> DataFrame:
        """
        Calcule les tendances et comparaisons temporelles
        
        Args:
            df: DataFrame avec données quotidiennes
        
        Returns:
            DataFrame enrichi avec tendances
        """
        logger.info("Calcul des tendances...")
        
        # Fenêtre pour moyennes mobiles
        window_7days = Window.orderBy("DATE").rowsBetween(-6, 0)
        window_30days = Window.orderBy("DATE").rowsBetween(-29, 0)
        
        if "TAVG" in df.columns:
            df = df.withColumn("TEMP_MA_7D", F.avg("TAVG").over(window_7days))
            df = df.withColumn("TEMP_MA_30D", F.avg("TAVG").over(window_30days))
        
        if "PRCP" in df.columns:
            df = df.withColumn("PRCP_MA_7D", F.avg("PRCP").over(window_7days))
        
        logger.info("Tendances calculées")
        
        return df
    
    def process_all(self, csv_path: str) -> Dict[str, DataFrame]:
        """
        Exécute le pipeline complet de traitement
        
        Args:
            csv_path: Chemin vers les données brutes
        
        Returns:
            Dictionnaire avec tous les DataFrames traités
        """
        logger.info("=" * 80)
        logger.info("DÉMARRAGE DU TRAITEMENT SPARK")
        logger.info("=" * 80)
        
        start_time = datetime.now()
        
        # 1. Lecture
        df_raw = self.read_raw_csv(csv_path)
        
        # 2. Nettoyage
        df_clean = self.clean_data(df_raw)
        
        # 3. Colonnes dérivées
        df_enriched = self.add_derived_columns(df_clean)
        
        # 4. Agrégations
        df_daily = self.calculate_daily_aggregations(df_enriched)
        df_monthly = self.calculate_monthly_aggregations(df_enriched)
        df_seasonal = self.calculate_seasonal_aggregations(df_enriched)
        
        # 5. Tendances
        df_trends = self.calculate_trends(df_daily)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info(f"TRAITEMENT TERMINÉ EN {duration:.2f}s")
        logger.info("=" * 80)
        
        return {
            'raw': df_raw,
            'clean': df_clean,
            'enriched': df_enriched,
            'daily': df_daily,
            'monthly': df_monthly,
            'seasonal': df_seasonal,
            'trends': df_trends
        }
    
    def stop(self):
        """Arrête la session Spark"""
        if self.spark:
            self.spark.stop()
            logger.info("Session Spark arrêtée")


def main():
    """Fonction principale pour test"""
    processor = WeatherSparkProcessor()
    
    try:
        # Trouver le dernier fichier CSV raw
        raw_dir = Path(processor.paths['raw_data'])
        csv_files = list(raw_dir.glob("*.csv"))
        
        if not csv_files:
            logger.error("Aucun fichier CSV trouvé. Exécutez d'abord l'ingestion.")
            return
        
        # Prendre le plus récent
        latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Fichier à traiter: {latest_csv}")
        
        # Traiter
        results = processor.process_all(str(latest_csv))
        
        # Afficher quelques statistiques
        logger.info("\n" + "=" * 80)
        logger.info("RÉSUMÉ DES DONNÉES TRAITÉES")
        logger.info("=" * 80)
        
        for name, df in results.items():
            logger.info(f"{name}: {df.count()} enregistrements")
        
        # Afficher un échantillon
        logger.info("\nÉchantillon des données enrichies:")
        results['enriched'].show(5, truncate=False)
        
    finally:
        processor.stop()


if __name__ == "__main__":
    main()


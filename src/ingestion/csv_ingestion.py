"""
Module d'ingestion de données CSV depuis NOAA NCEI
Source: https://www.ncei.noaa.gov/

Ce module gère l'ingestion résiliente de données météorologiques historiques
au format CSV avec checkpoints et sauvegarde des données brutes.
"""

import os
import sys
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import requests
from io import StringIO
import psycopg2
from psycopg2.extras import Json
import uuid
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Ajouter le répertoire parent au path pour les imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import get_logger
from src.utils.checkpoint import ProgressTracker

logger = get_logger(__name__)


class NOAACSVIngestion:
    """Classe pour l'ingestion de données CSV depuis NOAA"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.paths = self.config.get('paths', {})
        self.ingestion_config = self.config.get('ingestion', {})
        self.db_config = self._get_db_config()
        
        # Créer les répertoires nécessaires
        self._create_directories()
        
        logger.info("NOAACSVIngestion initialisé")
    
    def _load_config(self, config_path: str) -> dict:
        """Charge la configuration"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Erreur lors du chargement de la config: {e}")
            raise
    
    def _get_db_config(self) -> dict:
        """Récupère la configuration de la base de données depuis les variables d'environnement"""
        return {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'weather_metadata'),
            'user': os.getenv('POSTGRES_USER', 'weather_admin'),
            'password': os.getenv('POSTGRES_PASSWORD', 'weather_pass_2024')
        }
    
    def _create_directories(self) -> None:
        """Crée les répertoires nécessaires"""
        for path in self.paths.values():
            Path(path).mkdir(parents=True, exist_ok=True)
        logger.info("Répertoires créés")
    
    def download_noaa_data(self, 
                          station_id: str = "GHCND:USW00094728",  # JFK Airport, NY
                          start_date: str = "2020-01-01",
                          end_date: str = "2024-12-31") -> Optional[str]:
        """
        Télécharge des données depuis NOAA NCEI
        
        Note: Pour un vrai projet, vous devrez obtenir une clé API gratuite sur:
        https://www.ncdc.noaa.gov/cdo-web/token
        
        Args:
            station_id: ID de la station météo
            start_date: Date de début (YYYY-MM-DD)
            end_date: Date de fin (YYYY-MM-DD)
        
        Returns:
            Chemin du fichier téléchargé ou None
        """
        logger.info(f"Téléchargement des données NOAA pour la station {station_id}")
        logger.info(f"Période: {start_date} à {end_date}")
        
        # Essayer de récupérer le token depuis plusieurs sources
        api_token = (
            os.getenv('NOAA_API_TOKEN') or 
            self.config.get('noaa', {}).get('api_token')
        )
        
        if False:
            pass
        else:
            # Mode démo (données simulées)
            logger.warning("⚙️ Mode DÉMO: génération de données de démonstration")
            logger.warning("💡 Pour les vraies données NOAA:")
            logger.warning("   1. Obtenez un token gratuit sur: https://www.ncdc.noaa.gov/cdo-web/token")
            logger.warning("   2. Définissez la variable: NOAA_API_TOKEN=votre_token")
            logger.warning("   3. Ou ajoutez-le dans config/config.yaml")
            
            return self._create_sample_data()
    
    def _download_real_noaa_data(self, station_id: str, start_date: str, 
                                 end_date: str, api_token: str) -> str:
        """
        Télécharge les VRAIES données depuis l'API NOAA
        """
        import requests
        import time
        
        base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
        headers = {'token': api_token}
        
        all_data = []
        current_date = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        # L'API NOAA limite à 1000 résultats par requête
        # On découpe en périodes de 1 an
        logger.info("📡 Téléchargement des données par périodes (limites API)")
        
        while current_date < end:
            period_end = min(current_date + pd.DateOffset(years=1), end)
            
            params = {
                'datasetid': 'GHCND',
                'stationid': station_id,
                'startdate': current_date.strftime('%Y-%m-%d'),
                'enddate': period_end.strftime('%Y-%m-%d'),
                'limit': 1000,
                'units': 'metric'
            }
            
            logger.info(f"  → Requête : {params['startdate']} à {params['enddate']}")
            
            try:
                response = requests.get(base_url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                if 'results' in data:
                    all_data.extend(data['results'])
                    logger.info(f"    ✅ {len(data['results'])} enregistrements récupérés")
                else:
                    logger.warning(f"    ⚠️ Pas de données pour cette période")
                
                # Respecter la limite de 5 req/sec
                time.sleep(0.3)
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    logger.warning("    ⏳ Limite de taux atteinte, attente 60s...")
                    time.sleep(60)
                    continue
                else:
                    raise
            
            current_date = period_end
        
        if not all_data:
            raise ValueError("Aucune donnée récupérée de l'API NOAA")
        
        # Convertir en DataFrame
        df = pd.json_normalize(all_data)
        
        # Réorganiser les colonnes au format attendu
        df_formatted = self._format_noaa_response(df, station_id)
        
        # Sauvegarder
        raw_dir = Path(self.paths['raw_data'])
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_path = raw_dir / f"noaa_real_data_{timestamp}.csv"
        
        df_formatted.to_csv(csv_path, index=False)
        logger.info(f"✅ Vraies données NOAA sauvegardées: {csv_path} ({len(df_formatted)} enregistrements)")
        
        return str(csv_path)
    
    def _format_noaa_response(self, df: pd.DataFrame, station_id: str) -> pd.DataFrame:
        """
        Formate la réponse de l'API NOAA au format standard
        """
        # Pivoter les données (l'API retourne une ligne par métrique)
        df_pivot = df.pivot_table(
            index='date',
            columns='datatype',
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Renommer les colonnes
        column_mapping = {
            'date': 'DATE',
            'TMAX': 'TMAX',
            'TMIN': 'TMIN',
            'TAVG': 'TAVG',
            'PRCP': 'PRCP',
            'AWND': 'AWND',
        }
        
        df_pivot = df_pivot.rename(columns=column_mapping)
        
        # Ajouter les métadonnées de la station
        df_pivot['STATION'] = station_id
        df_pivot['STATION_NAME'] = 'NOAA Station'
        
        # Colonnes par défaut si manquantes
        for col in ['TMAX', 'TMIN', 'TAVG', 'PRCP', 'AWND', 'RHUM']:
            if col not in df_pivot.columns:
                df_pivot[col] = None
        
        # Calculer TAVG si manquant
        if 'TAVG' not in df_pivot.columns or df_pivot['TAVG'].isna().all():
            df_pivot['TAVG'] = (df_pivot['TMAX'] + df_pivot['TMIN']) / 2
        
        return df_pivot
    
    def _create_sample_data(self) -> str:
        """
        Crée des données d'exemple au format NOAA
        À remplacer par de vraies données téléchargées
        """
        import numpy as np
        
        logger.info("Génération de données d'exemple au format NOAA...")
        
        # Générer des données réalistes sur 5 ans
        date_range = pd.date_range(start='2020-01-01', end='2024-12-31', freq='D')
        
        # Simulation de températures réalistes (en degrés Celsius)
        np.random.seed(42)
        base_temp = 15  # Température de base
        seasonal_variation = 10 * np.sin(2 * np.pi * np.arange(len(date_range)) / 365.25)
        daily_variation = np.random.normal(0, 3, len(date_range))
        temperatures = base_temp + seasonal_variation + daily_variation
        
        # Autres variables météo
        humidity = np.clip(np.random.normal(70, 15, len(date_range)), 20, 100)
        precipitation = np.abs(np.random.exponential(2, len(date_range)))
        wind_speed = np.abs(np.random.gamma(2, 3, len(date_range)))
        
        # Créer le DataFrame
        df = pd.DataFrame({
            'STATION': 'GHCND:USW00094728',
            'STATION_NAME': 'JFK INTERNATIONAL AIRPORT, NY US',
            'DATE': date_range.strftime('%Y-%m-%d'),
            'LATITUDE': 40.6386,
            'LONGITUDE': -73.7622,
            'ELEVATION': 3.4,
            'TMAX': temperatures + np.random.uniform(2, 5, len(date_range)),  # Temp max
            'TMIN': temperatures - np.random.uniform(2, 5, len(date_range)),  # Temp min
            'TAVG': temperatures,  # Temp moyenne
            'PRCP': precipitation,  # Précipitations (mm)
            'AWND': wind_speed,  # Vitesse du vent (m/s)
            'RHUM': humidity,  # Humidité relative (%)
        })
        
        # Sauvegarder en CSV brut
        raw_dir = Path(self.paths['raw_data'])
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_path = raw_dir / f"noaa_weather_data_{timestamp}.csv"
        
        df.to_csv(csv_path, index=False)
        logger.info(f"Données d'exemple créées: {csv_path} ({len(df)} enregistrements)")
        
        return str(csv_path)
    
    def ingest_csv(self, csv_path: str, job_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Ingère un fichier CSV avec gestion de checkpoint
        
        Args:
            csv_path: Chemin vers le fichier CSV
            job_name: Nom du job (pour checkpoint)
        
        Returns:
            Statistiques d'ingestion
        """
        if job_name is None:
            job_name = f"csv_ingestion_{Path(csv_path).stem}"
        
        logger.info(f"Début de l'ingestion: {csv_path}")
        start_time = datetime.now()
        
        try:
            # Lire le fichier CSV
            df = pd.read_csv(csv_path)
            total_records = len(df)
            
            logger.info(f"Fichier chargé: {total_records} enregistrements")
            
            # Créer un tracker de progression
            tracker = ProgressTracker(
                job_name=job_name,
                total=total_records,
                checkpoint_interval=self.ingestion_config.get('checkpoint_interval', 1000)
            )
            
            # Validation et nettoyage de base
            valid_records = 0
            invalid_records = 0
            
            for idx, row in df.iterrows():
                try:
                    # Validation simple (à adapter selon vos besoins)
                    if pd.notna(row.get('DATE')) and pd.notna(row.get('TAVG')):
                        valid_records += 1
                        tracker.update(1, success=True)
                    else:
                        invalid_records += 1
                        tracker.update(1, success=False)
                except Exception as e:
                    logger.warning(f"Erreur ligne {idx}: {e}")
                    invalid_records += 1
                    tracker.update(1, success=False)
            
            # Sauvegarder les données brutes validées
            raw_validated_path = self._save_raw_data(df, csv_path)
            
            # Calculer les statistiques
            duration = (datetime.now() - start_time).total_seconds()
            file_size_mb = Path(csv_path).stat().st_size / (1024 * 1024)
            quality_score = (valid_records / total_records * 100) if total_records > 0 else 0
            
            stats = {
                'source_path': csv_path,
                'records_count': total_records,
                'records_valid': valid_records,
                'records_invalid': invalid_records,
                'file_size_mb': round(file_size_mb, 2),
                'processing_duration_seconds': round(duration, 2),
                'data_quality_score': round(quality_score, 2),
                'raw_validated_path': raw_validated_path,
                'data_start_date': df['DATE'].min() if 'DATE' in df.columns else None,
                'data_end_date': df['DATE'].max() if 'DATE' in df.columns else None
            }
            
            # Sauvegarder les métadonnées
            self._save_metadata(stats, 'CSV', Path(csv_path).name)
            
            # Compléter le tracking
            tracker.complete()
            
            logger.info(f"Ingestion terminée: {valid_records}/{total_records} enregistrements valides ({quality_score:.1f}%)")
            
            return stats
            
        except Exception as e:
            logger.error(f"Erreur lors de l'ingestion: {e}")
            raise
    
    def _save_raw_data(self, df: pd.DataFrame, original_path: str) -> str:
        """Sauvegarde une copie des données brutes"""
        raw_dir = Path(self.paths['raw_data'])
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"raw_{Path(original_path).stem}_{timestamp}.csv"
        raw_path = raw_dir / filename
        
        df.to_csv(raw_path, index=False)
        logger.info(f"Données brutes sauvegardées: {raw_path}")
        
        return str(raw_path)
    
    def _save_metadata(self, stats: Dict[str, Any], source_type: str, source_name: str) -> None:
        """Sauvegarde les métadonnées dans PostgreSQL"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            insert_query = """
            INSERT INTO ingestion_metadata (
                source_type, source_name, source_path,
                records_count, records_valid, records_invalid,
                file_size_mb, processing_duration_seconds, data_quality_score,
                status, data_start_date, data_end_date, additional_info
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cur.execute(insert_query, (
                source_type,
                source_name,
                stats.get('source_path'),
                stats.get('records_count'),
                stats.get('records_valid'),
                stats.get('records_invalid'),
                stats.get('file_size_mb'),
                stats.get('processing_duration_seconds'),
                stats.get('data_quality_score'),
                'SUCCESS',
                stats.get('data_start_date'),
                stats.get('data_end_date'),
                Json({'raw_validated_path': stats.get('raw_validated_path')})
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info("Métadonnées sauvegardées dans PostgreSQL")
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des métadonnées: {e}")
            logger.warning("Métadonnées non sauvegardées (base de données non accessible)")


def main():
    """Fonction principale pour test"""
    logger.info("=" * 80)
    logger.info("DÉMARRAGE DE L'INGESTION CSV NOAA")
    logger.info("=" * 80)
    
    ingestion = NOAACSVIngestion()
    
    # Télécharger/créer les données
    csv_path = ingestion.download_noaa_data()
    
    if csv_path:
        # Ingérer les données
        stats = ingestion.ingest_csv(csv_path)
        
        logger.info("=" * 80)
        logger.info("STATISTIQUES D'INGESTION")
        logger.info("=" * 80)
        for key, value in stats.items():
            logger.info(f"{key}: {value}")
    
    logger.info("=" * 80)
    logger.info("INGESTION TERMINÉE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()


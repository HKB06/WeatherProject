"""
Module d'ingestion de données météo via API temps réel
Source: Open-Meteo API (gratuite, sans clé nécessaire)
https://open-meteo.com/

Alternative: NOAA API (nécessite token gratuit)
https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation
"""

import os
import sys
import json
import yaml
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extras import Json

# Ajouter le répertoire parent au path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import get_logger

logger = get_logger(__name__)


class WeatherAPIIngestion:
    """Classe pour l'ingestion de données météo via API"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.paths = self.config.get('paths', {})
        self.api_config = self.config.get('data_sources', {}).get('api', {})
        self.db_config = self._get_db_config()
        
        # Créer les répertoires
        self._create_directories()
        
        logger.info("WeatherAPIIngestion initialisé")
    
    def _load_config(self, config_path: str) -> dict:
        """Charge la configuration"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Erreur lors du chargement de la config: {e}")
            raise
    
    def _get_db_config(self) -> dict:
        """Configuration base de données"""
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
    
    def fetch_current_weather(self, latitude: float, longitude: float, 
                             location_name: str = "Unknown") -> Optional[Dict[str, Any]]:
        """
        Récupère les données météo actuelles depuis Open-Meteo API
        
        Args:
            latitude: Latitude du lieu
            longitude: Longitude du lieu
            location_name: Nom du lieu
        
        Returns:
            Données météo ou None
        """
        logger.info(f"Récupération des données météo pour {location_name} ({latitude}, {longitude})")
        
        try:
            base_url = self.api_config.get('base_url', 'https://api.open-meteo.com/v1/forecast')
            
            params = {
                'latitude': latitude,
                'longitude': longitude,
                'current': 'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code',
                'hourly': 'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m',
                'timezone': 'auto',
                'forecast_days': 1
            }
            
            timeout = self.api_config.get('timeout', 30)
            
            response = requests.get(base_url, params=params, timeout=timeout)
            response.raise_for_status()
            
            data = response.json()
            
            # Enrichir avec métadonnées
            enriched_data = {
                'location_name': location_name,
                'latitude': latitude,
                'longitude': longitude,
                'fetch_timestamp': datetime.now().isoformat(),
                'data': data
            }
            
            logger.info(f"Données récupérées avec succès pour {location_name}")
            
            return enriched_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la requête API: {e}")
            return None
        except Exception as e:
            logger.error(f"Erreur inattendue: {e}")
            return None
    
    def fetch_multiple_locations(self, locations: Optional[List[Dict]] = None) -> List[Dict[str, Any]]:
        """
        Récupère les données pour plusieurs localisations
        
        Args:
            locations: Liste de dicts avec name, latitude, longitude
        
        Returns:
            Liste des données récupérées
        """
        if locations is None:
            # Utiliser les villes de la config
            locations = self.config.get('dashboard', {}).get('cities', [])
        
        logger.info(f"Récupération des données pour {len(locations)} localisations")
        
        results = []
        for location in locations:
            data = self.fetch_current_weather(
                latitude=location['latitude'],
                longitude=location['longitude'],
                location_name=location['name']
            )
            
            if data:
                results.append(data)
        
        logger.info(f"Données récupérées pour {len(results)}/{len(locations)} localisations")
        
        return results
    
    def save_raw_json(self, data: Dict[str, Any], location_name: str) -> str:
        """
        Sauvegarde les données brutes en JSON
        
        Args:
            data: Données à sauvegarder
            location_name: Nom de la localisation
        
        Returns:
            Chemin du fichier sauvegardé
        """
        raw_dir = Path(self.paths['raw_data'])
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"api_weather_{location_name.replace(' ', '_')}_{timestamp}.json"
        json_path = raw_dir / filename
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Données brutes sauvegardées: {json_path}")
        
        return str(json_path)
    
    def ingest_api_data(self, locations: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """
        Ingère les données API pour plusieurs localisations
        
        Args:
            locations: Liste des localisations
        
        Returns:
            Statistiques d'ingestion
        """
        logger.info("Début de l'ingestion API")
        start_time = datetime.now()
        
        # Récupérer les données
        results = self.fetch_multiple_locations(locations)
        
        # Sauvegarder chaque résultat
        saved_files = []
        for result in results:
            file_path = self.save_raw_json(result, result['location_name'])
            saved_files.append(file_path)
        
        # Statistiques
        duration = (datetime.now() - start_time).total_seconds()
        
        stats = {
            'source_type': 'API',
            'locations_requested': len(locations) if locations else 0,
            'locations_success': len(results),
            'processing_duration_seconds': round(duration, 2),
            'saved_files': saved_files,
            'fetch_timestamp': datetime.now().isoformat()
        }
        
        # Sauvegarder les métadonnées
        self._save_metadata(stats)
        
        logger.info(f"Ingestion API terminée: {len(results)} localisations en {duration:.2f}s")
        
        return stats
    
    def _save_metadata(self, stats: Dict[str, Any]) -> None:
        """Sauvegarde les métadonnées dans PostgreSQL"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            insert_query = """
            INSERT INTO ingestion_metadata (
                source_type, source_name, records_count, records_valid,
                processing_duration_seconds, status, additional_info
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cur.execute(insert_query, (
                'API',
                'Open-Meteo Weather API',
                stats.get('locations_requested', 0),
                stats.get('locations_success', 0),
                stats.get('processing_duration_seconds'),
                'SUCCESS',
                Json(stats)
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info("Métadonnées API sauvegardées dans PostgreSQL")
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des métadonnées: {e}")
            logger.warning("Métadonnées non sauvegardées (base de données non accessible)")


def main():
    """Fonction principale pour test"""
    logger.info("=" * 80)
    logger.info("DÉMARRAGE DE L'INGESTION API")
    logger.info("=" * 80)
    
    ingestion = WeatherAPIIngestion()
    
    # Ingérer les données
    stats = ingestion.ingest_api_data()
    
    logger.info("=" * 80)
    logger.info("STATISTIQUES D'INGESTION API")
    logger.info("=" * 80)
    for key, value in stats.items():
        if key != 'saved_files':
            logger.info(f"{key}: {value}")
    
    logger.info("=" * 80)
    logger.info("INGESTION API TERMINÉE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()


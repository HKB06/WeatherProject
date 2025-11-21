"""
Module d'ingestion de données météorologiques
Supporte CSV (NOAA historique) et API (temps réel)
"""

from src.ingestion.csv_ingestion import NOAACSVIngestion
from src.ingestion.api_ingestion import WeatherAPIIngestion

__all__ = [
    'NOAACSVIngestion',
    'WeatherAPIIngestion'
]


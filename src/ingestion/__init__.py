"""
Module d'ingestion de données météorologiques
Source : Open-Meteo Archive API (données historiques 2023-2025)
"""

from src.ingestion.api_ingestion import WeatherAPIIngestion

__all__ = [
    'WeatherAPIIngestion'
]

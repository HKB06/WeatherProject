"""
Module de logging centralisé pour le projet WeatherProject
Fournit un logger coloré avec plusieurs niveaux et sorties (console + fichier)
"""

import logging
import os
from datetime import datetime
from pathlib import Path
import colorlog
import yaml


class WeatherLogger:
    """Classe pour gérer le logging du projet"""
    
    def __init__(self, name: str, config_path: str = "config/config.yaml"):
        self.name = name
        self.config = self._load_config(config_path)
        self.logger = self._setup_logger()
    
    def _load_config(self, config_path: str) -> dict:
        """Charge la configuration depuis le fichier YAML"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            return config.get('logging', {})
        except Exception as e:
            print(f"Warning: Could not load config, using defaults: {e}")
            return {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'date_format': '%Y-%m-%d %H:%M:%S',
                'file_output': True,
                'console_output': True
            }
    
    def _setup_logger(self) -> logging.Logger:
        """Configure et retourne un logger"""
        logger = logging.getLogger(self.name)
        
        # Définir le niveau de log
        level = getattr(logging, self.config.get('level', 'INFO').upper())
        logger.setLevel(level)
        
        # Éviter la duplication des handlers
        if logger.handlers:
            return logger
        
        # Format pour les logs
        log_format = self.config.get('format', 
                                     '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        date_format = self.config.get('date_format', '%Y-%m-%d %H:%M:%S')
        
        # Handler console avec couleurs
        if self.config.get('console_output', True):
            console_handler = colorlog.StreamHandler()
            console_handler.setLevel(level)
            
            color_formatter = colorlog.ColoredFormatter(
                '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt=date_format,
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'red,bg_white',
                }
            )
            console_handler.setFormatter(color_formatter)
            logger.addHandler(console_handler)
        
        # Handler fichier
        if self.config.get('file_output', True):
            # Créer le dossier logs s'il n'existe pas
            log_dir = Path('logs')
            log_dir.mkdir(exist_ok=True)
            
            # Nom du fichier avec date
            log_file = log_dir / f"{self.name}_{datetime.now().strftime('%Y%m%d')}.log"
            
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(level)
            
            file_formatter = logging.Formatter(log_format, datefmt=date_format)
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        
        return logger
    
    def get_logger(self) -> logging.Logger:
        """Retourne l'instance du logger"""
        return self.logger


# Fonction helper pour obtenir rapidement un logger
def get_logger(name: str) -> logging.Logger:
    """
    Fonction utilitaire pour obtenir un logger configuré
    
    Args:
        name: Nom du logger (généralement __name__)
    
    Returns:
        Logger configuré
    """
    weather_logger = WeatherLogger(name)
    return weather_logger.get_logger()


# Exemple d'utilisation
if __name__ == "__main__":
    logger = get_logger("test")
    
    logger.debug("Message de debug")
    logger.info("Message d'information")
    logger.warning("Message d'avertissement")
    logger.error("Message d'erreur")
    logger.critical("Message critique")


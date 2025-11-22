"""
Module de persistance des données traitées
Sauvegarde en format Parquet avec partitionnement
Gestion des métadonnées dans PostgreSQL
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
import yaml
import psycopg2
from psycopg2.extras import Json
import uuid

from pyspark.sql import DataFrame

# Ajouter le répertoire parent au path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataPersistence:
    """Classe pour la persistance des données en Parquet"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.paths = self.config.get('paths', {})
        self.persistence_config = self.config.get('persistence', {})
        self.db_config = self._get_db_config()
        
        # Créer les répertoires
        self._create_directories()
        
        logger.info("DataPersistence initialisé")
    
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
        processed_dir = Path(self.paths['processed_data'])
        processed_dir.mkdir(parents=True, exist_ok=True)
    
    def save_to_parquet(self, 
                       df: DataFrame, 
                       dataset_name: str,
                       partition_by: Optional[List[str]] = None,
                       mode: str = "append") -> Dict[str, Any]:
        """
        Sauvegarde un DataFrame en format Parquet
        UTILISE PANDAS pour contourner le problème winutils.exe sur Windows
        
        Args:
            df: DataFrame Spark à sauvegarder
            dataset_name: Nom du dataset (ex: 'daily', 'monthly')
            partition_by: Colonnes de partitionnement (ignoré pour l'instant)
            mode: Mode de sauvegarde (append, overwrite, etc.)
        
        Returns:
            Métadonnées de la sauvegarde
        """
        logger.info(f"Sauvegarde du dataset '{dataset_name}' en Parquet (via Pandas)...")
        
        start_time = datetime.now()
        
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            import shutil
            
            # Chemin de destination
            output_path = Path(self.paths['processed_data']) / dataset_name
            
            # Supprimer le dossier existant si mode overwrite
            if mode == 'overwrite' and output_path.exists():
                shutil.rmtree(output_path)
            
            # Créer le répertoire
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Convertir Spark DataFrame en Pandas
            logger.info(f"Conversion Spark -> Pandas pour '{dataset_name}'...")
            pandas_df = df.toPandas()
            record_count = len(pandas_df)
            
            # Sauvegarder en Parquet avec Pandas/PyArrow
            compression = self.persistence_config.get('compression', 'snappy')
            parquet_file = output_path / f"{dataset_name}.parquet"
            
            logger.info(f"Écriture du fichier Parquet: {parquet_file}")
            pandas_df.to_parquet(
                parquet_file,
                engine='pyarrow',
                compression=compression,
                index=False
            )
            
            # Calculer les statistiques
            duration = (datetime.now() - start_time).total_seconds()
            
            # Taille du fichier
            file_size = parquet_file.stat().st_size if parquet_file.exists() else 0
            size_mb = file_size / (1024 * 1024)
            
            metadata = {
                'dataset_name': dataset_name,
                'output_path': str(output_path),
                'record_count': record_count,
                'size_mb': round(size_mb, 2),
                'partitioned_by': [],  # Pas de partitionnement avec Pandas
                'compression': compression,
                'mode': mode,
                'duration_seconds': round(duration, 2),
                'saved_at': datetime.now().isoformat()
            }
            
            logger.info(f" Dataset '{dataset_name}' sauvegardé: {record_count} enregistrements, {size_mb:.2f} MB en {duration:.2f}s")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde en Parquet: {e}")
            raise
    
    def save_all_datasets(self, dataframes: Dict[str, DataFrame]) -> List[Dict[str, Any]]:
        """
        Sauvegarde tous les datasets traités
        
        Args:
            dataframes: Dictionnaire {nom: DataFrame}
        
        Returns:
            Liste des métadonnées de sauvegarde
        """
        logger.info(f"Sauvegarde de {len(dataframes)} datasets...")
        
        all_metadata = []
        
        for name, df in dataframes.items():
            try:
                # DÉSACTIVATION TEMPORAIRE DU PARTITIONNEMENT
                # (bug Spark 3.3.0 sur Windows avec partitionnement)
                partition_by = None
                # if name in ['daily', 'trends']:
                #     partition_by = ['YEAR', 'MONTH'] if 'YEAR' in df.columns else None
                # elif name in ['monthly']:
                #     partition_by = ['YEAR'] if 'YEAR' in df.columns else None
                
                metadata = self.save_to_parquet(
                    df=df,
                    dataset_name=name,
                    partition_by=partition_by,
                    mode='overwrite'  # Écraser pour les traitements complets
                )
                
                all_metadata.append(metadata)
                
            except Exception as e:
                logger.error(f"Erreur lors de la sauvegarde du dataset '{name}': {e}")
                continue
        
        logger.info(f"{len(all_metadata)}/{len(dataframes)} datasets sauvegardés avec succès")
        
        return all_metadata
    
    def save_transformation_metadata(self, 
                                    transformation_name: str,
                                    input_records: int,
                                    output_records: int,
                                    processing_time: float,
                                    output_path: str,
                                    config: Optional[Dict] = None,
                                    ingestion_id: Optional[str] = None) -> str:
        """
        Sauvegarde les métadonnées de transformation dans PostgreSQL
        
        Args:
            transformation_name: Nom de la transformation
            input_records: Nombre d'enregistrements en entrée
            output_records: Nombre d'enregistrements en sortie
            processing_time: Temps de traitement en secondes
            output_path: Chemin de sortie
            config: Configuration utilisée
            ingestion_id: ID de l'ingestion source
        
        Returns:
            ID de la transformation créée
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            transformation_id = str(uuid.uuid4())
            records_dropped = input_records - output_records if output_records < input_records else 0
            
            insert_query = """
            INSERT INTO spark_transformations (
                id, ingestion_id, transformation_name, transformation_type,
                input_records, output_records, records_dropped,
                processing_time_seconds, config, output_path, status, completed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """
            
            cur.execute(insert_query, (
                transformation_id,
                ingestion_id,
                transformation_name,
                'AGGREGATION',
                input_records,
                output_records,
                records_dropped,
                processing_time,
                Json(config or {}),
                output_path,
                'SUCCESS',
                datetime.now()
            ))
            
            conn.commit()
            result_id = cur.fetchone()[0]
            cur.close()
            conn.close()
            
            logger.info(f"Métadonnées de transformation sauvegardées: {result_id}")
            
            return result_id
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des métadonnées de transformation: {e}")
            logger.warning("Métadonnées non sauvegardées (base de données non accessible)")
            return None
    
    def save_aggregations_metadata(self,
                                  aggregation_type: str,
                                  metric_name: str,
                                  period_start: str,
                                  period_end: str,
                                  values: Dict[str, float],
                                  transformation_id: Optional[str] = None,
                                  location: Optional[str] = None) -> str:
        """
        Sauvegarde les métadonnées des agrégations calculées
        
        Args:
            aggregation_type: Type d'agrégation (DAILY, MONTHLY, etc.)
            metric_name: Nom de la métrique
            period_start: Date de début
            period_end: Date de fin
            values: Valeurs calculées (mean, min, max, etc.)
            transformation_id: ID de la transformation
            location: Localisation
        
        Returns:
            ID de l'agrégation créée
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            insert_query = """
            INSERT INTO calculated_aggregations (
                transformation_id, aggregation_type, metric_name,
                period_start, period_end, value_mean, value_min, value_max,
                value_std, value_count, location
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """
            
            cur.execute(insert_query, (
                transformation_id,
                aggregation_type.upper(),
                metric_name,
                period_start,
                period_end,
                values.get('mean'),
                values.get('min'),
                values.get('max'),
                values.get('std'),
                values.get('count'),
                location
            ))
            
            conn.commit()
            result_id = cur.fetchone()[0]
            cur.close()
            conn.close()
            
            return result_id
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des métadonnées d'agrégation: {e}")
            return None
    
    def read_parquet(self, dataset_name: str, spark_session) -> Optional[DataFrame]:
        """
        Lit un dataset Parquet
        
        Args:
            dataset_name: Nom du dataset
            spark_session: Session Spark
        
        Returns:
            DataFrame ou None
        """
        try:
            parquet_path = Path(self.paths['processed_data']) / dataset_name
            
            if not parquet_path.exists():
                logger.warning(f"Dataset '{dataset_name}' non trouvé: {parquet_path}")
                return None
            
            df = spark_session.read.parquet(str(parquet_path))
            logger.info(f"Dataset '{dataset_name}' chargé: {df.count()} enregistrements")
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de la lecture de '{dataset_name}': {e}")
            return None
    
    def list_available_datasets(self) -> List[str]:
        """
        Liste les datasets disponibles
        
        Returns:
            Liste des noms de datasets
        """
        try:
            processed_dir = Path(self.paths['processed_data'])
            
            if not processed_dir.exists():
                return []
            
            datasets = [d.name for d in processed_dir.iterdir() if d.is_dir()]
            logger.info(f"Datasets disponibles: {', '.join(datasets)}")
            
            return datasets
            
        except Exception as e:
            logger.error(f"Erreur lors du listage des datasets: {e}")
            return []
    
    def get_dataset_info(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtient les informations sur un dataset
        
        Args:
            dataset_name: Nom du dataset
        
        Returns:
            Informations sur le dataset
        """
        try:
            dataset_path = Path(self.paths['processed_data']) / dataset_name
            
            if not dataset_path.exists():
                return None
            
            # Calculer la taille totale
            total_size = sum(f.stat().st_size for f in dataset_path.rglob('*') if f.is_file())
            size_mb = total_size / (1024 * 1024)
            
            # Compter les fichiers
            file_count = len(list(dataset_path.rglob('*.parquet')))
            
            # Date de dernière modification
            latest_mod = max(f.stat().st_mtime for f in dataset_path.rglob('*') if f.is_file())
            last_modified = datetime.fromtimestamp(latest_mod).isoformat()
            
            return {
                'name': dataset_name,
                'path': str(dataset_path),
                'size_mb': round(size_mb, 2),
                'file_count': file_count,
                'last_modified': last_modified
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des infos du dataset: {e}")
            return None


def main():
    """Fonction principale pour test"""
    from src.processing.spark_processing import WeatherSparkProcessor
    
    logger.info("=" * 80)
    logger.info("TEST DE LA PERSISTANCE")
    logger.info("=" * 80)
    
    # Créer le processor et la persistance
    processor = WeatherSparkProcessor()
    persistence = DataPersistence()
    
    try:
        # Trouver un fichier CSV
        raw_dir = Path(processor.paths['raw_data'])
        csv_files = list(raw_dir.glob("*.csv"))
        
        if not csv_files:
            logger.error("Aucun fichier CSV trouvé")
            return
        
        latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
        
        # Traiter
        results = processor.process_all(str(latest_csv))
        
        # Sauvegarder
        datasets_to_save = {
            'daily': results['daily'],
            'monthly': results['monthly'],
            'seasonal': results['seasonal']
        }
        
        metadata_list = persistence.save_all_datasets(datasets_to_save)
        
        logger.info("\n" + "=" * 80)
        logger.info("RÉSUMÉ DE LA SAUVEGARDE")
        logger.info("=" * 80)
        
        for metadata in metadata_list:
            logger.info(f"\nDataset: {metadata['dataset_name']}")
            logger.info(f"  - Enregistrements: {metadata['record_count']}")
            logger.info(f"  - Taille: {metadata['size_mb']} MB")
            logger.info(f"  - Durée: {metadata['duration_seconds']}s")
        
        # Lister les datasets disponibles
        datasets = persistence.list_available_datasets()
        logger.info(f"\nDatasets disponibles: {', '.join(datasets)}")
        
    finally:
        processor.stop()


if __name__ == "__main__":
    main()


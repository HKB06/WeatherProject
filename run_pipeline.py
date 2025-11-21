"""Pipeline complet WeatherProject: Ingestion -> Traitement -> Persistance"""

import sys
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
sys.path.append(str(Path(__file__).parent))

from src.utils.logger import get_logger
from src.ingestion.csv_ingestion import NOAACSVIngestion
from src.ingestion.api_ingestion import WeatherAPIIngestion
from src.processing.spark_processing import WeatherSparkProcessor
from src.persistence.data_persistence import DataPersistence

logger = get_logger(__name__)


def print_banner():
    banner = """
    ================================================================
              WEATHERPROJECT - BIG DATA PIPELINE             
               Architecture DataLake avec Apache Spark          
    ================================================================
    """
    try:
        print(banner)
    except UnicodeEncodeError:
        print("WEATHERPROJECT - BIG DATA PIPELINE")
        print("Architecture DataLake avec Apache Spark")


def run_ingestion_phase():
    logger.info("=" * 80)
    logger.info("PHASE 1: INGESTION DES DONNÉES")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    logger.info("\n📥 Ingestion CSV - Données historiques NOAA")
    csv_ingestion = NOAACSVIngestion()
    csv_path = csv_ingestion.download_noaa_data()
    
    if csv_path:
        csv_stats = csv_ingestion.ingest_csv(csv_path)
        logger.info(f"✅ CSV ingéré: {csv_stats['records_valid']} enregistrements valides")
    else:
        logger.error("❌ Échec du téléchargement des données CSV")
        return None
    
    logger.info("\n📥 Ingestion API - Données temps réel")
    api_ingestion = WeatherAPIIngestion()
    api_stats = api_ingestion.ingest_api_data()
    logger.info(f"✅ API ingérée: {api_stats['locations_success']} localisations")
    
    duration = time.time() - start_time
    logger.info(f"\n⏱️  Phase d'ingestion terminée en {duration:.2f}s")
    
    return csv_path


def run_processing_phase(csv_path):
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 2: TRAITEMENT SPARK")
    logger.info("=" * 80)
    
    start_time = time.time()
    processor = WeatherSparkProcessor()
    
    try:
        logger.info("\n⚙️  Traitement des données avec Spark...")
        results = processor.process_all(csv_path)
        
        duration = time.time() - start_time
        logger.info(f"\n⏱️  Phase de traitement terminée en {duration:.2f}s")
        
        logger.info("\n📊 Résumé des datasets créés:")
        for name, df in results.items():
            count = df.count()
            logger.info(f"  - {name}: {count} enregistrements")
        
        return results, processor
        
    except Exception as e:
        logger.error(f"❌ Erreur lors du traitement: {e}")
        processor.stop()
        return None, None


def run_persistence_phase(results, processor):
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 3: PERSISTANCE DES DONNÉES")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    try:
        persistence = DataPersistence()
        
        datasets_to_save = {
            'daily': results['daily'],
            'monthly': results['monthly'],
            'seasonal': results['seasonal']
        }
        
        logger.info("\n💾 Sauvegarde des datasets en Parquet...")
        metadata_list = persistence.save_all_datasets(datasets_to_save)
        
        duration = time.time() - start_time
        logger.info(f"\n⏱️  Phase de persistance terminée en {duration:.2f}s")
        
        logger.info("\n📦 Datasets sauvegardés:")
        for metadata in metadata_list:
            logger.info(f"  - {metadata['dataset_name']}: {metadata['size_mb']} MB")
        
        return metadata_list
        
    finally:
        if processor:
            processor.stop()
            logger.info("Session Spark arrêtée")


def display_final_summary(total_duration):
    logger.info("\n" + "=" * 80)
    logger.info("🎉 PIPELINE TERMINÉ AVEC SUCCÈS")
    logger.info("=" * 80)
    logger.info(f"\n⏱️  Durée totale: {total_duration:.2f}s ({total_duration/60:.1f} minutes)")
    logger.info("\n📍 Prochaines étapes:")
    logger.info("  1. Vérifier les données dans data/processed/")
    logger.info("  2. Consulter les métadonnées dans PostgreSQL")
    logger.info("  3. Lancer le dashboard: python dashboard/app.py")
    logger.info("  4. Accéder au dashboard: http://localhost:5000")
    logger.info("\n🔍 Spark UI: http://localhost:8080")
    logger.info("\n" + "=" * 80)


def main():
    print_banner()
    total_start_time = time.time()
    
    try:
        csv_path = run_ingestion_phase()
        if not csv_path:
            logger.error("❌ Échec de la phase d'ingestion")
            return 1
        
        results, processor = run_processing_phase(csv_path)
        if not results or not processor:
            logger.error("❌ Échec de la phase de traitement")
            return 1
        
        metadata_list = run_persistence_phase(results, processor)
        if not metadata_list:
            logger.error("❌ Échec de la phase de persistance")
            return 1
        
        total_duration = time.time() - total_start_time
        display_final_summary(total_duration)
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Pipeline interrompu par l'utilisateur")
        return 1
    except Exception as e:
        logger.error(f"\n❌ Erreur fatale: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())


"""Dashboard Flask pour l'analyse de données météo"""

import os
import sys
from pathlib import Path
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.logger import get_logger
from src.persistence.data_persistence import DataPersistence

logger = get_logger(__name__)

app = Flask(__name__)
CORS(app)

app.config['JSON_SORT_KEYS'] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

persistence = DataPersistence()


def clean_dataframe_for_json(df):
    df = df.replace([np.nan, np.inf, -np.inf], None)
    return df


def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', 5432)),
            database=os.getenv('POSTGRES_DB', 'weather_metadata'),
            user=os.getenv('POSTGRES_USER', 'weather_admin'),
            password=os.getenv('POSTGRES_PASSWORD', 'weather_pass_2024')
        )
        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion à la base de données: {e}")
        return None


def read_parquet_to_pandas(dataset_name: str) -> pd.DataFrame:
    try:
        dataset_dir = Path(persistence.paths['processed_data']) / dataset_name
        dataset_file = dataset_dir / f"{dataset_name}.parquet"
        
        if not dataset_file.exists():
            logger.warning(f"Fichier Parquet non trouvé: {dataset_file}")
            return pd.DataFrame()
        
        df = pd.read_parquet(dataset_file)
        logger.info(f"Dataset '{dataset_name}' chargé: {len(df)} enregistrements")
        return df
        
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du dataset: {e}")
        return pd.DataFrame()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/datasets')
def get_datasets():
    try:
        datasets = persistence.list_available_datasets()
        datasets_info = []
        for dataset_name in datasets:
            info = persistence.get_dataset_info(dataset_name)
            if info:
                datasets_info.append(info)
        
        return jsonify({
            'success': True,
            'count': len(datasets_info),
            'datasets': datasets_info
        })
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ingestion-stats')
def get_ingestion_stats():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT 
                source_type,
                COUNT(*) as total_ingestions,
                SUM(records_count) as total_records,
                AVG(data_quality_score) as avg_quality_score,
                MAX(ingestion_timestamp) as last_ingestion
            FROM ingestion_metadata
            GROUP BY source_type
        """)
        
        stats = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'stats': [dict(row) for row in stats]
        })
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/weather/daily')
def get_daily_weather():
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        limit = int(request.args.get('limit', 5000))
        
        df = read_parquet_to_pandas('daily')
        if df.empty:
            return jsonify({'success': False, 'error': 'No data available'}), 404
        
        if 'DATE' in df.columns:
            df['DATE'] = pd.to_datetime(df['DATE'])
        
        if start_date:
            df = df[df['DATE'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['DATE'] <= pd.to_datetime(end_date)]
        
        df = df.sort_values('DATE', ascending=False).head(limit)
        df['DATE'] = df['DATE'].dt.strftime('%Y-%m-%d')
        df = clean_dataframe_for_json(df)
        
        return jsonify({
            'success': True,
            'count': len(df),
            'data': df.to_dict(orient='records')
        })
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/weather/monthly')
def get_monthly_weather():
    try:
        df = read_parquet_to_pandas('monthly')
        if df.empty:
            return jsonify({'success': False, 'error': 'No data available'}), 404
        
        if 'YEAR' in df.columns and 'MONTH' in df.columns:
            df = df.sort_values(['YEAR', 'MONTH'], ascending=False)
        
        df = clean_dataframe_for_json(df)
        return jsonify({'success': True, 'count': len(df), 'data': df.to_dict(orient='records')})
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/weather/seasonal')
def get_seasonal_weather():
    try:
        df = read_parquet_to_pandas('seasonal')
        if df.empty:
            return jsonify({'success': False, 'error': 'No data available'}), 404
        
        if 'YEAR' in df.columns:
            df = df.sort_values('YEAR', ascending=False)
        
        df = clean_dataframe_for_json(df)
        return jsonify({'success': True, 'count': len(df), 'data': df.to_dict(orient='records')})
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/weather/temperature-trends')
def get_temperature_trends():
    try:
        df = read_parquet_to_pandas('daily')
        if df.empty:
            return jsonify({'success': False, 'error': 'No data available'}), 404
        
        temp_columns = ['DATE', 'TEMP_AVG', 'TEMP_MIN', 'TEMP_MAX']
        available_columns = [col for col in temp_columns if col in df.columns]
        
        if not available_columns:
            return jsonify({'success': False, 'error': 'Temperature data not available'}), 404
        
        df = df[available_columns].copy()
        if 'DATE' in df.columns:
            df['DATE'] = pd.to_datetime(df['DATE']).dt.strftime('%Y-%m-%d')
        
        df = df.tail(365 * 3)
        df = clean_dataframe_for_json(df)
        
        return jsonify({'success': True, 'count': len(df), 'data': df.to_dict(orient='records')})
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/weather/precipitation-analysis')
def get_precipitation_analysis():
    try:
        df = read_parquet_to_pandas('daily')
        if df.empty or 'PRECIPITATION_TOTAL' not in df.columns:
            return jsonify({'success': False, 'error': 'No precipitation data available'}), 404
        
        if 'YEAR' in df.columns and 'MONTH' in df.columns:
            monthly_precip = df.groupby(['YEAR', 'MONTH']).agg({'PRECIPITATION_TOTAL': 'sum'}).reset_index()
            monthly_precip['DATE'] = pd.to_datetime(monthly_precip[['YEAR', 'MONTH']].assign(DAY=1)).dt.strftime('%Y-%m')
            monthly_precip = clean_dataframe_for_json(monthly_precip)
            
            return jsonify({'success': True, 'count': len(monthly_precip), 'data': monthly_precip.to_dict(orient='records')})
        
        return jsonify({'success': False, 'error': 'Insufficient data for analysis'}), 404
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/weather/statistics')
def get_weather_statistics():
    """Retourne des statistiques générales"""
    try:
        df = read_parquet_to_pandas('daily')
        
        if df.empty:
            return jsonify({'success': False, 'error': 'No data available'}), 404
        
        stats = {}
        
        # Statistiques de température
        if 'TEMP_AVG' in df.columns:
            temp_mean = df['TEMP_AVG'].mean()
            temp_min = df['TEMP_AVG'].min()
            temp_max = df['TEMP_AVG'].max()
            temp_std = df['TEMP_AVG'].std()
            stats['temperature'] = {
                'mean': float(temp_mean) if pd.notna(temp_mean) else 0,
                'min': float(temp_min) if pd.notna(temp_min) else 0,
                'max': float(temp_max) if pd.notna(temp_max) else 0,
                'std': float(temp_std) if pd.notna(temp_std) else 0
            }
        
        # Statistiques de précipitations
        if 'PRECIPITATION_TOTAL' in df.columns:
            precip_total = df['PRECIPITATION_TOTAL'].sum()
            precip_mean = df['PRECIPITATION_TOTAL'].mean()
            precip_max = df['PRECIPITATION_TOTAL'].max()
            stats['precipitation'] = {
                'total': float(precip_total) if pd.notna(precip_total) else 0,
                'mean': float(precip_mean) if pd.notna(precip_mean) else 0,
                'max': float(precip_max) if pd.notna(precip_max) else 0,
                'days_with_rain': int((df['PRECIPITATION_TOTAL'] > 0).sum())
            }
        
        # Statistiques d'humidité
        if 'HUMIDITY_AVG' in df.columns:
            hum_mean = df['HUMIDITY_AVG'].mean()
            hum_min = df['HUMIDITY_AVG'].min()
            hum_max = df['HUMIDITY_AVG'].max()
            stats['humidity'] = {
                'mean': float(hum_mean) if pd.notna(hum_mean) else 0,
                'min': float(hum_min) if pd.notna(hum_min) else 0,
                'max': float(hum_max) if pd.notna(hum_max) else 0
            }
        
        # Période couverte
        if 'DATE' in df.columns:
            df['DATE'] = pd.to_datetime(df['DATE'])
            stats['period'] = {
                'start': df['DATE'].min().strftime('%Y-%m-%d'),
                'end': df['DATE'].max().strftime('%Y-%m-%d'),
                'total_days': len(df)
            }
        
        return jsonify({
            'success': True,
            'statistics': stats
        })
        
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/health')
def health_check():
    return jsonify({
        'success': True,
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })


if __name__ == '__main__':
    logger.info("=" * 80)
    logger.info("DÉMARRAGE DU DASHBOARD WEATHERPROJECT")
    logger.info("=" * 80)
    
    host = app.config.get('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV', 'production') == 'development'
    
    logger.info(f"Dashboard: http://{host}:{port}")
    logger.info(f"Mode debug: {debug}")
    
    app.run(host=host, port=port, debug=debug)


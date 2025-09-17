import requests
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from bs4 import BeautifulSoup
import pdfplumber
import time

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EudaimoniaPredictorGenerator:
    # ... (el resto de tu código existente)
    # No es necesario cambiar la mayoría del código, solo la sección if __name__ == "__main__":
    
    # Asegúrate de que los métodos están actualizados como en la respuesta anterior

if __name__ == "__main__":
    # Obtener API keys de variables de entorno
    media_cloud_key = os.environ.get('MEDIA_CLOUD_KEY')
    newsapi_key = os.environ.get('NEWSAPI_KEY')
    
    # Nuevo: Leer la lista de países de una variable de entorno
    country_codes_str = os.environ.get('COUNTRIES_TO_PROCESS')
    if country_codes_str:
        country_list = country_codes_str.split(',')
    else:
        # Fallback si no se define la variable (para pruebas locales)
        logging.warning("⚠️ No se definió la variable COUNTRIES_TO_PROCESS. Usando la lista completa como fallback.")
        country_list = [
            'USA', 'CHN', 'IND', 'BRA', 'RUS', 'JPN', 'DEU', 'GBR', 'CAN', 'FRA',
            'ITA', 'AUS', 'MEX', 'KOR', 'SAU', 'TUR', 'EGY', 'NGA', 'PAK', 'IDN',
            'VNM', 'PHL', 'ARG', 'COL', 'POL', 'ESP', 'IRN', 'ZAF', 'UKR', 'THA',
            'VEN', 'CHL', 'PER', 'MYS', 'ROU', 'SWE', 'BEL', 'NLD', 'GRC', 'CZE',
            'PRT', 'DNK', 'FIN', 'NOR', 'SGP', 'AUT', 'CHE', 'IRL', 'NZL', 'HKG',
            'ISR', 'ARE'
        ]
    
    generator = EudaimoniaPredictorGenerator(country_list)
    result = generator.generate_indices_json(
        media_cloud_key=media_cloud_key, 
        newsapi_key=newsapi_key
    )
    
    # Resumen de procesamiento
    countries_with_fresh_data = sum(1 for code, data in result['results'].items() 
                                  if 'daily_data' in data and any(d['data_available'] for d in data['daily_data'].values()))
    
    logging.info(f"📈 Procesamiento completado:")
    logging.info(f"   - Países procesados: {len(result['results'])}")
    logging.info(f"   - Países con datos frescos: {countries_with_fresh_data}")
    logging.info(f"   - Datos frescos disponibles: {result['metadata']['fresh_data_available']}")

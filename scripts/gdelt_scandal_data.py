import requests
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class GDELTScandalDataGenerator:
    """
    Genera un archivo JSON con métricas de escándalos y corrupción basadas en GDELT,
    utilizando palabras clave en múltiples idiomas.
    """
    DATA_DIR = 'data'
    OUTPUT_FILE = os.path.join(DATA_DIR, 'data_gdelt.json')
    
    # URL de la API de GDELT - punto final corregido
    GDELT_BASE_URL = "https://api.gdeltproject.org/api/v2/gkg/timeline"

    def __init__(self, country_codes: list):
        self.country_codes = country_codes
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=365) # Último año
        
        # Diccionario de términos de búsqueda en diferentes idiomas
        self.search_terms = {
            'default': ['corruption', 'scandal', 'bribery', 'money laundering', 'abuse of power'],
            'ESP': ['corrupción', 'escándalo', 'soborno', 'lavado de dinero', 'abuso de poder'],
            'CHN': ['腐败', '丑闻', '贿赂', '洗钱'], # Chino (mandarín)
            'RUS': ['коррупция', 'скандал', 'взяточничество', 'отмывание денег'], # Ruso
            'DEU': ['Korruption', 'Skandal', 'Bestechung', 'Geldwäsche'], # Alemán
            'FRA': ['corruption', 'scandale', 'pot-de-vin', 'blanchiment d\'argent'], # Francés
            'ITA': ['corruzione', 'scandalo', 'tangenti', 'riciclaggio di denaro'], # Italiano
            'PRT': ['corrupção', 'escândalo', 'suborno', 'lavagem de dinheiro'], # Portugués
            'KOR': ['부패', '스캔들', '뇌물', '돈세탁'], # Coreano
            'JPN': ['汚職', 'スキャンダル', '賄賂', 'マネーロンダリング'], # Japonés
            'SAU': ['فساد', 'فضيحة', 'رشوة', 'غسيل أموال'], # Árabe
            'TUR': ['yolsuzluk', 'skandal', 'rüşvet', 'kara para aklama'], # Turco
            'VNM': ['tham nhũng', 'bê bối', 'hối lộ', 'rửa tiền'], # Vietnamita
            'THA': ['การทุจริต', 'เรื่องอื้อฉาว', 'การติดสินบน', 'การฟอกเงิน'] # Tailandés
        }
        
    def _fetch_gdelt_data(self, country_code: str) -> int:
        """
        Consulta la API de GDELT para obtener el número de eventos de escándalos.
        Selecciona las palabras clave basadas en el idioma del país.
        """
        terms = self.search_terms.get(country_code, self.search_terms['default'])
        query = f"sourcecountry:{country_code} ({' OR '.join(terms)})"
        
        # Parámetros corregidos para la API de GDELT
        params = {
            'query': query,
            'mode': 'TimelineVol',
            'format': 'json',
            'timebin': '15min', # '1min' es demasiado granular y puede ser inestable
            'startdatetime': self.start_date.strftime('%Y%m%d%H%M%S'),
            'enddatetime': self.end_date.strftime('%Y%m%d%H%M%S')
        }
        
        logging.info(f"Buscando datos de GDELT para {country_code} con términos: {terms}")
        try:
            response = requests.get(self.GDELT_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # GDELT retorna un diccionario con el total de artículos
            total_events = int(data.get('timeline').get('events', {}).get('total', 0))
            logging.info(f"✅ Eventos encontrados para {country_code}: {total_events}")
            return total_events
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error al conectar con la API de GDELT para {country_code}: {e}")
            return 0
        except (KeyError, IndexError, ValueError):
            logging.warning(f"⚠️ No se encontraron datos de escándalos en GDELT para {country_code}.")
            return 0

    def generate_gdelt_json(self) -> None:
        """
        Genera el archivo JSON con los datos de GDELT para todos los países.
        """
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR)
            logging.info(f"Directorio '{self.DATA_DIR}' creado.")

        all_gdelt_data = {}
        for country_code in self.country_codes:
            event_count = self._fetch_gdelt_data(country_code)
            all_gdelt_data[country_code] = {
                "scandal_events_last_year": event_count,
                "data_source": "GDELT"
            }
        
        final_data = {
            "metadata": {
                "source": "GDELT Project API",
                "purpose": "Proxy para medir transgresión y opacidad",
                "processing_date": datetime.now().isoformat(),
                "query_terms_per_language": {lang: terms for lang, terms in self.search_terms.items() if lang != 'default'},
                "time_range": f"{self.start_date.date()} a {self.end_date.date()}"
            },
            "results": all_gdelt_data
        }
        
        with open(self.OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)
        
        logging.info(f"✅ Datos de GDELT guardados en {self.OUTPUT_FILE}")

if __name__ == "__main__":
    country_list = [
        'USA', 'CHN', 'IND', 'BRA', 'RUS', 'JPN', 'DEU', 'GBR', 'CAN', 'FRA',
        'ITA', 'AUS', 'MEX', 'KOR', 'SAU', 'TUR', 'EGY', 'NGA', 'PAK', 'IDN',
        'VNM', 'PHL', 'ARG', 'COL', 'POL', 'ESP', 'IRN', 'ZAF', 'UKR', 'THA',
        'VEN', 'CHL', 'PER', 'MYS', 'ROU', 'SWE', 'BEL', 'NLD', 'GRC', 'CZE',
        'PRT', 'DNK', 'FIN', 'NOR', 'SGP', 'AUT', 'CHE', 'IRL', 'NZL', 'HKG',
        'ISR', 'ARE'
    ]
    
    generator = GDELTScandalDataGenerator(country_list)
    generator.generate_gdelt_json()

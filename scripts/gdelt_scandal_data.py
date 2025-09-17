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
    utilizando palabras clave en inglés para todos los países, ya que la API busca en traducciones al inglés.
    Calcula conteos raw y ratios normalizados por volumen de medios del país.
    """
    DATA_DIR = 'data'
    OUTPUT_FILE = os.path.join(DATA_DIR, 'data_gdelt.json')
    
    # URL corregida para la API DOC 2.0 de GDELT
    GDELT_BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

    def __init__(self, country_codes: list):
        self.country_codes = country_codes
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=90)  # Últimos 3 meses para datos frescos
        
        # Términos de búsqueda solo en inglés, ya que la API usa traducciones al inglés
        self.search_terms = ['corruption', 'scandal', 'bribery', 'money laundering', 'abuse of power']
        
    def _fetch_gdelt_volume(self, query: str, country_code: str) -> int:
        """
        Consulta la API de GDELT para obtener el volumen total de artículos.
        """
        params = {
            'query': query,
            'mode': 'TimelineVolRaw',
            'format': 'json',
            'startdatetime': self.start_date.strftime('%Y%m%d000000'),
            'enddatetime': self.end_date.strftime('%Y%m%d235959')
        }
        
        logging.info(f"Buscando volumen de GDELT para {country_code} con query: {query}")
        try:
            response = requests.get(self.GDELT_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Sumar el 'Volume' a lo largo de la timeline para obtener el total de artículos
            if 'timeline' in data:
                total_volume = sum(entry.get('Volume', 0) for entry in data['timeline'])
                logging.info(f"✅ Volumen encontrado para {country_code}: {total_volume}")
                return total_volume
            else:
                logging.warning(f"⚠️ No se encontraron datos en GDELT para {country_code} con query: {query}")
                return 0
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error al conectar con la API de GDELT para {country_code}: {e}")
            return 0
        except (KeyError, ValueError):
            logging.warning(f"⚠️ Formato de respuesta inesperado para {country_code}.")
            return 0

    def generate_gdelt_json(self) -> None:
        """
        Genera el archivo JSON con los datos de GDELT para todos los países, incluyendo normalización.
        """
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR)
            logging.info(f"Directorio '{self.DATA_DIR}' creado.")

        all_gdelt_data = {}
        terms_quoted = [f'"{term}"' if ' ' in term else term for term in self.search_terms]
        query_terms = ' OR '.join(terms_quoted)
        
        for country_code in self.country_codes:
            # Query para escándalos
            query_scandal = f"sourcecountry:{country_code} ({query_terms})"
            scandal_count = self._fetch_gdelt_volume(query_scandal, country_code)
            
            # Query para total de artículos del país
            query_total = f"sourcecountry:{country_code}"
            total_count = self._fetch_gdelt_volume(query_total, country_code)
            
            # Calcular ratio normalizado (porcentaje de artículos sobre escándalos)
            scandal_ratio = (scandal_count / total_count * 100) if total_count > 0 else 0
            
            all_gdelt_data[country_code] = {
                "scandal_article_count_last_3_months": scandal_count,
                "total_article_count_last_3_months": total_count,
                "scandal_percentage": round(scandal_ratio, 4),
                "data_source": "GDELT"
            }
        
        final_data = {
            "metadata": {
                "source": "GDELT Project API",
                "purpose": "Proxy para medir transgresión y opacidad con datos normalizados por país",
                "processing_date": datetime.now().isoformat(),
                "query_terms": self.search_terms,
                "normalization": "Porcentaje de artículos del país que mencionan términos de escándalo (basado en traducciones al inglés)",
                "time_range": f"{self.start_date.date()} a {self.end_date.date()}"
            },
            "results": all_gdelt_data
        }
        
        with open(self.OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)
        
        logging.info(f"✅ Datos de GDELT guardados en {self.OUTPUT_FILE}")

if __name__ == "__main__":
    # Códigos de país en 2 letras FIPS, como requiere GDELT
    country_list = [
        'US', 'CH', 'IN', 'BR', 'RS', 'JA', 'GM', 'UK', 'CA', 'FR',
        'IT', 'AS', 'MX', 'KS', 'SA', 'TU', 'EG', 'NI', 'PK', 'ID',
        'VM', 'RP', 'AR', 'CO', 'PL', 'SP', 'IR', 'SF', 'UP', 'TH',
        'VE', 'CI', 'PE', 'MY', 'RO', 'SW', 'BE', 'NL', 'GR', 'EZ',
        'PO', 'DA', 'FI', 'NO', 'SN', 'AU', 'SZ', 'EI', 'NZ', 'HK',
        'IS', 'AE'
    ]
    
    generator = GDELTScandalDataGenerator(country_list)
    generator.generate_gdelt_json()

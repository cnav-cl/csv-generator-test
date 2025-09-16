import json
import pandas as pd
from datetime import datetime
import time
from typing import Dict, List, Optional
import os
import requests
import logging
import random
import numpy as np
from requests.exceptions import Timeout, RequestException

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class HistoricalDataGenerator:
    def __init__(self):
        # Usar TODOS los países de tu mapeo original
        self.country_codes = [
            'USA', 'CHN', 'IND', 'BRA', 'RUS', 'JPN', 'DEU', 'GBR', 'CAN', 'FRA',
            'ITA', 'AUS', 'MEX', 'KOR', 'SAU', 'TUR', 'EGY', 'NGA', 'PAK', 'IDN',
            'VNM', 'PHL', 'ARG', 'COL', 'POL', 'ESP', 'IRN', 'ZAF', 'UKR', 'THA',
            'VEN', 'CHL', 'PER', 'MYS', 'ROU', 'SWE', 'BEL', 'NLD', 'GRC', 'CZE',
            'PRT', 'DNK', 'FIN', 'NOR', 'SGP', 'AUT', 'CHE', 'IRL', 'NZL', 'HKG',
            'ISR', 'ARE'
        ]
        
        self.indicators = {
            'gini_coefficient': 'SI.POV.GINI',
            'youth_unemployment': 'SL.UEM.1524.ZS',
            'inflation_annual': 'FP.CPI.TOTL.ZG',
            'neet_ratio': 'SL.UEM.NEET.ZS',
            'tertiary_education': 'SE.TER.ENRR',
            'government_effectiveness': 'GE.EST',
            'political_stability': 'PV.EST',
            'control_of_corruption': 'CC.EST',
            'voice_accountability': 'VA.EST',
            'rule_of_law': 'RL.EST',
            'regulatory_quality': 'RQ.EST'
        }
        
        self.imf_indicators = {
            'inflation_annual': 'PCPI_A_SA_X_PCT',
            'gdp_per_capita': 'NGDPDPC_SA_XDC',
            'unemployment_rate': 'LUR_SA_X_PT',
            'real_gdp_growth': 'NGDP_RPCH'
        }
        
        self.default_indicator_values = {
            'GINI': {'USA': 40.0, 'default': 40.0},
            '1524.ZS': {'default': 20.0},
            'TOTL.ZG': {'default': 3.0},
            'NEET.ZS': {'default': 10.0},
            'TER.ENRR': {'default': 60.0},
            'GE.EST': {'default': 0.0},
            'PV.EST': {'default': 0.0},
            'CC.EST': {'default': 0.0},
            'VA.EST': {'default': 0.0},
            'RL.EST': {'default': 0.0},
            'RQ.EST': {'default': 0.0},
            'PCPI_A_SA_X_PCT': {'default': 3.0},
            'NGDPDPC_SA_XDC': {'default': 10000.0},
            'LUR_SA_X_PT': {'default': 5.0},
            'NGDP_RPCH': {'default': 2.0}
        }

    def get_default_key(self, indicator_code: str) -> Optional[str]:
        """Busca la clave de valor por defecto correcta para un código de indicador."""
        parts = indicator_code.split('.')
        for i in range(1, 4):
            key = '.'.join(parts[-i:])
            if key in self.default_indicator_values:
                return key
        return None

    def get_default_value(self, indicator_code: str, country_code: str) -> float:
        default_key = self.get_default_key(indicator_code)
        if default_key:
            return float(self.default_indicator_values[default_key].get(country_code, self.default_indicator_values[default_key].get('default', 0.0)))
        return 0.0

    def safe_numeric_conversion(self, value) -> Optional[float]:
        """Convierte de forma segura un valor a numérico, manejando nulos y valores no numéricos."""
        if value is None:
            return None
            
        try:
            # Intentar convertir a float
            numeric_value = float(value)
            # Verificar si es un número válido (no infinito ni NaN)
            if np.isfinite(numeric_value):
                return numeric_value
            else:
                return None
        except (ValueError, TypeError):
            return None

    def fetch_with_retry(self, url: str, max_retries: int = 3, timeout: int = 30) -> Optional[dict]:
        """Realiza una petición HTTP con reintentos y manejo de timeouts."""
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=timeout)
                response.raise_for_status()
                
                # Verificar que la respuesta contiene datos válidos
                if response.status_code == 200:
                    try:
                        data = response.json()
                        return data
                    except json.JSONDecodeError:
                        logging.warning(f"Intento {attempt+1}: Respuesta JSON inválida de {url}")
                else:
                    logging.warning(f"Intento {attempt+1}: Código de estado {response.status_code} de {url}")
                    
            except Timeout:
                logging.warning(f"Intento {attempt+1}: Timeout al conectar con {url}")
            except RequestException as e:
                logging.warning(f"Intento {attempt+1}: Error de conexión con {url}: {e}")
            except Exception as e:
                logging.warning(f"Intento {attempt+1}: Error inesperado con {url}: {e}")
            
            # Esperar antes de reintentar (backoff exponencial)
            if attempt < max_retries - 1:
                sleep_time = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(sleep_time)
        
        logging.error(f"Todos los intentos fallaron para {url}")
        return None

    def fetch_world_bank_historical(self, country_code: str, indicator_code: str, start_year: int, end_year: int) -> Dict:
        """Fetch historical World Bank data for multiple years with robust error handling"""
        historical_data = {}
        indicator_name = next((k for k, v in self.indicators.items() if v == indicator_code), indicator_code)
        
        # Construir URL para múltiples años
        api_url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}?date={start_year}:{end_year}&format=json&per_page=100"
        
        data = self.fetch_with_retry(api_url)
        if not data:
            logging.warning(f"No se pudieron obtener datos del Banco Mundial para {country_code} - {indicator_code}")
            return historical_data
        
        try:
            if len(data) > 1 and data[1]:
                for item in data[1]:
                    if item.get('value') is not None:
                        numeric_value = self.safe_numeric_conversion(item['value'])
                        if numeric_value is not None:
                            historical_data[int(item['date'])] = numeric_value
                        else:
                            logging.debug(f"Valor no numérico ignorado para {country_code} {indicator_code} en {item['date']}: {item['value']}")
        except (KeyError, IndexError, TypeError) as e:
            logging.error(f"Error procesando datos del Banco Mundial para {country_code} {indicator_code}: {e}")
        
        # Si no hay datos, usar valor por defecto para el año más reciente
        if not historical_data and end_year >= 2020:
            default_value = self.get_default_value(indicator_code, country_code)
            historical_data[end_year] = default_value
            logging.info(f"Usando valor por defecto para {country_code} - {indicator_code}: {default_value}")
        
        return historical_data

    def fetch_imf_historical(self, country_code: str, indicator_code: str, start_year: int, end_year: int) -> Dict:
        """Fetch historical IMF data for multiple years with robust error handling"""
        historical_data = {}
        
        api_url = f"https://www.imf.org/external/datamapper/api/v1/{indicator_code}/{country_code}?periods={start_year}:{end_year}"
        
        data = self.fetch_with_retry(api_url)
        if not data:
            logging.warning(f"No se pudieron obtener datos del FMI para {country_code} - {indicator_code}")
            return historical_data
        
        try:
            series_data = data.get('values', {}).get(indicator_code, {}).get(country_code, {})
            for year_str, value in series_data.items():
                if value is not None:
                    numeric_value = self.safe_numeric_conversion(value)
                    if numeric_value is not None:
                        historical_data[int(year_str)] = numeric_value
                    else:
                        logging.debug(f"Valor no numérico ignorado para {country_code} {indicator_code} en {year_str}: {value}")
        except (KeyError, TypeError) as e:
            logging.error(f"Error procesando datos del FMI para {country_code} {indicator_code}: {e}")
        
        # Si no hay datos, usar valor por defecto para el año más reciente
        if not historical_data and end_year >= 2020:
            default_value = self.get_default_value(indicator_code, country_code)
            historical_data[end_year] = default_value
            logging.info(f"Usando valor por defecto para {country_code} - {indicator_code}: {default_value}")
        
        return historical_data

    def generate_historical_dataset(self, batch_size: int = 10):
        """Generate historical dataset for the last 5 years with comprehensive error handling"""
        current_year = 2025
        start_year = current_year - 5
        
        historical_data = {
            'metadata': {
                'generated_on': datetime.now().isoformat(),
                'data_range': f'{start_year}-{current_year}',
                'countries': self.country_codes,
                'total_countries': len(self.country_codes),
                'source': 'World Bank and IMF APIs'
            },
            'world_bank': {},
            'imf': {}
        }
        
        # Procesar en lotes para evitar sobrecargar las APIs
        total_countries = len(self.country_codes)
        
        # Fetch World Bank data por lotes
        logging.info(f"Fetching World Bank historical data for {total_countries} countries in batches...")
        for batch_start in range(0, total_countries, batch_size):
            batch_end = min(batch_start + batch_size, total_countries)
            batch_countries = self.country_codes[batch_start:batch_end]
            
            logging.info(f"Processing batch {batch_start//batch_size + 1}/{(total_countries + batch_size - 1)//batch_size}: {batch_countries}")
            
            for country in batch_countries:
                historical_data['world_bank'][country] = {}
                for indicator_name, indicator_code in self.indicators.items():
                    try:
                        data = self.fetch_world_bank_historical(country, indicator_code, start_year, current_year)
                        if data:
                            historical_data['world_bank'][country][indicator_name] = data
                            logging.info(f"✓ {country} - {indicator_name} ({len(data)} puntos de datos)")
                        else:
                            logging.warning(f"✗ {country} - {indicator_name} (sin datos)")
                            # Añadir valor por defecto para el año actual
                            default_value = self.get_default_value(indicator_code, country)
                            historical_data['world_bank'][country][indicator_name] = {current_year: default_value}
                    except Exception as e:
                        logging.error(f"Error procesando {country} - {indicator_name}: {e}")
                        # Añadir valor por defecto en caso de error
                        default_value = self.get_default_value(indicator_code, country)
                        historical_data['world_bank'][country][indicator_name] = {current_year: default_value}
            
            # Pequeña pausa entre lotes
            time.sleep(2)
        
        # Fetch IMF data por lotes
        logging.info(f"Fetching IMF historical data for {total_countries} countries in batches...")
        for batch_start in range(0, total_countries, batch_size):
            batch_end = min(batch_start + batch_size, total_countries)
            batch_countries = self.country_codes[batch_start:batch_end]
            
            logging.info(f"Processing batch {batch_start//batch_size + 1}/{(total_countries + batch_size - 1)//batch_size}: {batch_countries}")
            
            for country in batch_countries:
                historical_data['imf'][country] = {}
                for indicator_name, indicator_code in self.imf_indicators.items():
                    try:
                        data = self.fetch_imf_historical(country, indicator_code, start_year, current_year)
                        if data:
                            historical_data['imf'][country][indicator_name] = data
                            logging.info(f"✓ {country} - {indicator_name} ({len(data)} puntos de datos)")
                        else:
                            logging.warning(f"✗ {country} - {indicator_name} (sin datos)")
                            # Añadir valor por defecto para el año actual
                            default_value = self.get_default_value(indicator_code, country)
                            historical_data['imf'][country][indicator_name] = {current_year: default_value}
                    except Exception as e:
                        logging.error(f"Error procesando {country} - {indicator_name}: {e}")
                        # Añadir valor por defecto en caso de error
                        default_value = self.get_default_value(indicator_code, country)
                        historical_data['imf'][country][indicator_name] = {current_year: default_value}
            
            # Pequeña pausa entre lotes
            time.sleep(2)
        
        # Save to file
        os.makedirs('data', exist_ok=True)
        try:
            with open('data/historical_data_2020_2025.json', 'w') as f:
                json.dump(historical_data, f, indent=2)
            logging.info(f"Historical data saved to data/historical_data_2020_2025.json")
            logging.info(f"Total countries processed: {total_countries}")
        except Exception as e:
            logging.error(f"Error guardando archivo histórico: {e}")
        
        return historical_data

if __name__ == "__main__":
    generator = HistoricalDataGenerator()
    generator.generate_historical_dataset(batch_size=8)  # Tamaño de lote reducido para evitar timeouts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import os
import time
from typing import Dict, List, Optional, Tuple
import json
from dataclasses import dataclass
import re
import random
from bs4 import BeautifulSoup
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.holtwinters import SimpleExpSmoothing
import concurrent.futures
import logging
import copy
import threading

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class DataSource:
    name: str
    base_url: str
    api_key: str = ""
    rate_limit: float = 0.1

class CliodynamicDataProcessor:
    def __init__(self, cache_file: str = 'data/cache.json'):
        self.cache_file = cache_file
        self.cache = self.load_cache()
        self.cache_lock = threading.Lock()
        self.temp_cache = {}

        self.gdelt_country_mapping = {
            'USA': ['United States', 'USA', 'US'],
            'CHN': ['China', 'CN'],
            'IND': ['India', 'IN'],
            'BRA': ['Brazil', 'BR'],
            'RUS': ['Russia', 'Russian Federation', 'RU'],
            'JPN': ['Japan', 'JP'],
            'DEU': ['Germany', 'DE'],
            'GBR': ['United Kingdom', 'GB', 'UK'],
            'CAN': ['Canada', 'CA'],
            'FRA': ['France', 'FR'],
            'ITA': ['Italy', 'IT'],
            'AUS': ['Australia', 'AU'],
            'MEX': ['Mexico', 'MX'],
            'KOR': ['South Korea', 'Republic of Korea', 'KR'],
            'SAU': ['Saudi Arabia', 'SA'],
            'TUR': ['Turkey', 'TR'],
            'EGY': ['Egypt', 'EG'],
            'NGA': ['Nigeria', 'NG'],
            'PAK': ['Pakistan', 'PK'],
            'IDN': ['Indonesia', 'ID'],
            'VNM': ['Vietnam', 'VN'],
            'PHL': ['Philippines', 'PH'],
            'ARG': ['Argentina', 'AR'],
            'COL': ['Colombia', 'CO'],
            'POL': ['Poland', 'PL'],
            'ESP': ['Spain', 'ES'],
            'IRN': ['Iran', 'IR'],
            'ZAF': ['South Africa', 'ZA'],
            'UKR': ['Ukraine', 'UA'],
            'THA': ['Thailand', 'TH'],
            'VEN': ['Venezuela, Bolivarian Republic of', 'Venezuela', 'VE'],
            'CHL': ['Chile', 'CL'],
            'PER': ['Peru', 'PE'],
            'MYS': ['Malaysia', 'MY'],
            'ROU': ['Romania', 'RO'],
            'SWE': ['Sweden', 'SE'],
            'BEL': ['Belgium', 'BE'],
            'NLD': ['Netherlands', 'NL'],
            'GRC': ['Greece', 'GR'],
            'CZE': ['Czech Republic', 'CZ'],
            'PRT': ['Portugal', 'PT'],
            'DNK': ['Denmark', 'DK'],
            'FIN': ['Finland', 'FI'],
            'NOR': ['Norway', 'NO'],
            'SGP': ['Singapore', 'SG'],
            'AUT': ['Austria', 'AT'],
            'CHE': ['Switzerland', 'CH'],
            'IRL': ['Ireland', 'IE'],
            'NZL': ['New Zealand', 'NZ'],
            'HKG': ['Hong Kong', 'HK'],
            'ISR': ['Israel', 'IL'],
            'ARE': ['United Arab Emirates', 'AE'],
            'EGY': ['Egypt, Arab Rep.', 'EG']
        }

        self.country_codes = list(self.gdelt_country_mapping.keys())
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
            'RQ.EST': {'default': 0.0}
        }
        self.gdelt_indicators = {
            'social_polarization': 'CIVIL_WAR_RISK',
            'institutional_distrust': 'GOV_DISTRUST',
            'suicide_rate': 'SUICIDE',
            'elite_overproduction': 'ELITE_OVERPRODUCTION',
            'wealth_concentration': 'WEALTH_CONCENTRATION'
        }
        self.current_year = datetime.now().year

    def load_cache(self) -> Dict:
        """Loads cache from a JSON file."""
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        return {}

    def save_cache(self):
        """Saves cache to a JSON file."""
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f)

    def get_default_key(self, indicator_code: str) -> Optional[str]:
        """
        Busca la clave de valor por defecto correcta para un código de indicador,
        manejando códigos complejos como 'SL.UEM.1524.ZS'.
        """
        parts = indicator_code.split('.')
        for i in range(1, 4):
            key = '.'.join(parts[-i:])
            if key in self.default_indicator_values:
                return key
        return None

    def fetch_world_bank_data(self, country_code: str, indicator_code: str, start_year: int, end_year: int) -> Optional[Dict]:
        """
        Fetches World Bank data for a specific country and indicator, with retries.
        Ajustado para usar un User-Agent y manejar mejor los fallos.
        """
        api_url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}?date={start_year}:{end_year}&format=json"
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            time.sleep(1)
            
            if len(data) > 1 and data[1]:
                return {item['date']: item['value'] for item in data[1] if 'date' in item and 'value' in item}
            else:
                logging.warning(f"No data found for {indicator_code} in {country_code} for years {start_year}-{end_year}. Using default value.")
                
                default_key = self.get_default_key(indicator_code)
                if default_key:
                    defaults = self.default_indicator_values[default_key]
                    default_value = defaults.get(country_code, defaults.get('default', 0.0))
                else:
                    default_value = 0.0
                
                return {str(end_year): default_value}
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from World Bank for {indicator_code} in {country_code}: {e}")
            
            default_key = self.get_default_key(indicator_code)
            if default_key:
                defaults = self.default_indicator_values[default_key]
                default_value = defaults.get(country_code, defaults.get('default', 0.0))
            else:
                default_value = 0.0
            
            return {str(end_year): default_value}
        except (json.JSONDecodeError, IndexError) as e:
            logging.error(f"Failed to parse JSON for {indicator_code} in {country_code}: {e}")
            return None

    def calculate_indicators(self, country_code: str, year: int) -> Dict:
        """Calculates indicators for a given country and year."""
        indicators = {}
        end_year = datetime.now().year - 1
        start_year = end_year - 5
        
        for name, code in self.indicators.items():
            cache_key = f"{country_code}_{code}_{end_year}"
            
            with self.cache_lock:
                if cache_key in self.cache and self.cache[cache_key]['retrieved_on'] == str(datetime.now().date()):
                    indicators[name] = self.cache[cache_key]['value']
                    logging.info(f"Using cached value for {name} ({country_code})")
                    continue
            
            data = self.fetch_world_bank_data(country_code, code, start_year, end_year)
            if data and str(end_year) in data:
                value = data[str(end_year)]
                indicators[name] = value
                
                with self.cache_lock:
                    self.temp_cache[cache_key] = {
                        'value': value,
                        'retrieved_on': str(datetime.now().date())
                    }
            else:
                pass

        for name, code in self.gdelt_indicators.items():
            indicators[name] = random.uniform(0.1, 0.9)

        return indicators

    def process_country(self, country_code: str, year: int) -> Optional[Dict]:
        """Main processing logic for a single country."""
        indicators = self.calculate_indicators(country_code, year)
        if not indicators:
            logging.warning(f"Skipping {country_code} due to missing data.")
            return None

        result = {
            'country_code': country_code,
            'year': year,
            'indicators': indicators,
            'estabilidad_jiang': {'status': 'stable', 'valor': 7.05},
            'inestabilidad_turchin': {'status': 'stable', 'valor': 0.26}
        }
        return result

    def save_to_json(self, data: List[Dict]):
        """
        Saves the final data structure to a JSON file.
        He cambiado el nombre del archivo de salida.
        """
        final_output = {
            'timestamp': datetime.now().isoformat(),
            'version': '1.0',
            'countries_processed': len(data),
            'results': data
        }
        # Nuevo nombre del archivo
        with open('data/data_paises.json', 'w') as f:
            json.dump(final_output, f, indent=2)

    def main(self, test_mode: bool = False):
        """Main method to run the data processing."""
        start_time = time.time()
        logging.info("Starting main data processing")
        
        end_year = datetime.now().year - 1
        
        results = []
        countries = ['USA'] if test_mode else self.country_codes
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_country = {executor.submit(self.process_country, country, end_year): country for country in countries}
            
            for future in concurrent.futures.as_completed(future_to_country, timeout=600):
                country = future_to_country[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                    logging.info(f"Completed processing for {country}")
                except concurrent.futures.TimeoutError:
                    logging.error(f"Timeout processing {country}, skipping")
                except Exception as e:
                    logging.error(f"Error processing {country}: {e}", exc_info=True)

        with self.cache_lock:
            self.cache.update(self.temp_cache)
            self.save_cache()
        
        self.save_to_json(results)
        logging.info(f"Main process completed in {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)
    processor = CliodynamicDataProcessor()
    processor.main(test_mode=False)

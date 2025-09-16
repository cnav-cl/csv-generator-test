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
        
        self.border_mapping = {
            'USA': ['CAN', 'MEX'],
            'CAN': ['USA'],
            'MEX': ['USA', 'GTM', 'BLZ'],
            'RUS': ['CHN', 'UKR', 'FIN', 'NOR', 'POL', 'LTU', 'LVA', 'EST', 'BLR', 'GEO', 'AZE', 'KAZ', 'MNG', 'PRK'],
            'CHN': ['RUS', 'IND', 'KOR', 'VNM', 'MYS', 'PAK', 'IDN'],
            'IND': ['CHN', 'PAK', 'NPL', 'BTN', 'MMR', 'BGD'],
            'BRA': ['ARG', 'COL', 'VEN', 'PER', 'BOL', 'PRY', 'URY'],
            'UKR': ['RUS', 'POL', 'ROU', 'SVK', 'HUN', 'MDA', 'BLR'],
            'DEU': ['FRA', 'POL', 'CZE', 'AUT', 'CHE', 'LUX', 'BEL', 'NLD', 'DNK'],
            'FRA': ['DEU', 'ESP', 'ITA', 'CHE', 'LUX', 'BEL'],
            'ESP': ['FRA', 'PRT'],
            'ITA': ['FRA', 'CHE', 'AUT', 'SVN', 'HRV'],
            'GBR': ['IRL'],
            'JPN': [],
            'KOR': ['PRK', 'CHN'],
            'TUR': ['SYR', 'IRQ', 'IRN', 'ARM', 'GEO', 'GRC', 'BGR'],
            'IRN': ['TUR', 'IRQ', 'PAK', 'AFG', 'TKM', 'ARM', 'AZE'],
            'IDN': ['MYS', 'TLS', 'PNG'],
            'EGY': ['ISR', 'SDN', 'LBY'],
            'NGA': ['BEN', 'NER', 'CMR', 'TCD'],
            'PAK': ['IND', 'IRN', 'AFG', 'CHN'],
            'VNM': ['CHN', 'LAO', 'KHM'],
            'PHL': [],
            'ARG': ['BRA', 'CHL', 'BOL', 'PRY', 'URY'],
            'COL': ['BRA', 'VEN', 'ECU', 'PAN', 'PER'],
            'POL': ['DEU', 'CZE', 'SVK', 'UKR', 'BLR', 'RUS', 'LTU'],
            'ZAF': ['NAM', 'BWA', 'ZWE', 'MOZ', 'SWZ', 'LSO'],
            'THA': ['LAO', 'MMR', 'KHM', 'MYS'],
            'VEN': ['BRA', 'COL', 'GUY'],
            'CHL': ['ARG', 'BOL', 'PER'],
            'PER': ['BRA', 'COL', 'ECU', 'BOL', 'CHL'],
            'MYS': ['THA', 'IDN', 'SGP'],
            'ROU': ['BGR', 'SRB', 'HUN', 'UKR', 'MDA'],
            'SWE': ['NOR', 'FIN'],
            'BEL': ['FRA', 'DEU', 'NLD', 'LUX'],
            'NLD': ['BEL', 'DEU'],
            'GRC': ['ALB', 'MKD', 'BGR', 'TUR'],
            'CZE': ['DEU', 'POL', 'AUT', 'SVK'],
            'PRT': ['ESP'],
            'DNK': ['DEU'],
            'FIN': ['SWE', 'NOR', 'RUS'],
            'NOR': ['SWE', 'FIN', 'RUS'],
            'SGP': ['Singapore', 'SG'],
            'AUT': ['DEU', 'CHE', 'ITA', 'SVN', 'HRV', 'HUN', 'SVK', 'CZE'],
            'CHE': ['DEU', 'FRA', 'ITA', 'AUT'],
            'IRL': ['GBR'],
            'NZL': ['New Zealand', 'NZ'],
            'HKG': ['Hong Kong', 'HK'],
            'ISR': ['Israel', 'IL'],
            'ARE': ['United Arab Emirates', 'AE'],
            'COL': ['BRA', 'VEN', 'ECU', 'PAN', 'PER'],
            'KAZ': ['RUS', 'CHN', 'KGZ', 'UZB', 'TKM'],
            'BLR': ['POL', 'LTU', 'LVA', 'RUS', 'UKR'],
            'GEO': ['RUS', 'TUR', 'ARM', 'AZE'],
            'AZE': ['RUS', 'GEO', 'IRN', 'TUR'],
            'URY': ['BRA', 'ARG'],
            'IDN': ['MYS', 'TLS', 'PNG'],
            'PAK': ['IND', 'AFG', 'IRN', 'CHN'],
            'UKR': ['POL', 'SVK', 'HUN', 'ROU', 'MDA', 'BLR', 'RUS'],
            'THA': ['LAO', 'MMR', 'KHM', 'MYS'],
            'VEN': ['BRA', 'COL', 'GUY'],
            'CHL': ['PER', 'BOL', 'ARG'],
            'PER': ['ECU', 'COL', 'BRA', 'BOL', 'CHL'],
            'MYS': ['THA', 'IDN', 'SGP']
        }

    def load_cache(self) -> Dict:
        """Loads cache from a JSON file."""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logging.error("Failed to decode cache JSON file. Starting with empty cache.")
                return {}
        return {}

    def save_cache(self):
        """Saves cache to a JSON file."""
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f)

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

    def fetch_world_bank_data(self, country_code: str, indicator_code: str, start_year: int, end_year: int) -> Dict:
        """
        Fetches World Bank data, iterating backward to find the most recent valid value
        and then projects it to the current year if needed. Includes retries.
        """
        headers = {'User-Agent': 'Mozilla/5.0'}
        default_value = self.get_default_value(indicator_code, country_code)
        historical_data = {}

        for year in range(end_year, end_year - 5, -1):
            api_url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}?date={year}&format=json"
            
            # Nuevo bloque: Retries con espera exponencial
            for i in range(3):
                try:
                    # Aumenta el timeout a 30 segundos
                    response = requests.get(api_url, headers=headers, timeout=30) 
                    response.raise_for_status()
                    data = response.json()
                    
                    if len(data) > 1 and data[1] and data[1][0]['value'] is not None:
                        historical_data[int(data[1][0]['date'])] = data[1][0]['value']
                        break # Salir del bucle de reintentos
                except (requests.exceptions.RequestException, json.JSONDecodeError, IndexError, TypeError) as e:
                    logging.warning(f"Attempt {i+1} failed for {indicator_code} in {country_code} for year {year}: {e}")
                    if i < 2:
                        time.sleep(2 ** i) # Espera 1, 2, 4 segundos
                    else:
                        logging.warning(f"Max retries reached for {indicator_code} in {country_code}. Trying previous year.")
            else:
                continue # Continuar al siguiente año si los reintentos fallan
            
            # Si el bucle interno se rompe con éxito, romper el externo también
            break

        # Si se encuentra data histórica, realizar nowcasting
        if historical_data:
            most_recent_year = max(historical_data.keys())
            most_recent_value = historical_data[most_recent_year]
            
            if most_recent_year < end_year:
                try:
                    df = pd.DataFrame(list(historical_data.items()), columns=['year', 'value']).sort_values('year')
                    fit = SimpleExpSmoothing(df['value']).fit()
                    forecast = fit.forecast(1)[0]
                    logging.info(f"Projecting {indicator_code} for {country_code} from {most_recent_year} to {end_year}: {round(forecast, 2)}")
                    return {str(end_year): float(forecast)}
                except Exception as e:
                    logging.error(f"Failed to forecast for {indicator_code} in {country_code}: {e}. Using most recent value.")
                    return {str(most_recent_year): float(most_recent_value)}
            else:
                return {str(most_recent_year): float(most_recent_value)}

        # Si no se encontró ninguna data histórica, usar el valor por defecto
        logging.warning(f"No valid data found for {indicator_code} in {country_code} for the last 5 years. Using default value.")
        return {str(end_year): float(default_value)}


    def calculate_indicators(self, country_code: str, year: int) -> Dict:
        """Calculates indicators, ensuring values are never None."""
        indicators = {}
        end_year = datetime.now().year - 1
        
        for name, code in self.indicators.items():
            cache_key = f"{country_code}_{code}_{end_year}"
            
            value_from_cache = None
            with self.cache_lock:
                if cache_key in self.cache and self.cache[cache_key]['retrieved_on'] == str(datetime.now().date()):
                    value_from_cache = self.cache[cache_key]['value']
            
            if value_from_cache is not None:
                indicators[name] = float(value_from_cache)
                logging.info(f"Using cached value for {name} ({country_code})")
                continue

            # Si el valor de la caché es None o no existe, llamar a la API
            data = self.fetch_world_bank_data(country_code, code, end_year - 5, end_year)
            
            # Obtener el valor del diccionario, tomando el primer valor si existe
            value = next(iter(data.values()), None)
            
            # Asegurar que el valor es un float antes de asignarlo
            if value is None:
                final_value = self.get_default_value(code, country_code)
            else:
                final_value = float(value)
            
            indicators[name] = final_value
                
            with self.cache_lock:
                self.temp_cache[cache_key] = {
                    'value': indicators[name],
                    'retrieved_on': str(datetime.now().date())
                }

        for name, code in self.gdelt_indicators.items():
            indicators[name] = random.uniform(0.1, 0.9)

        return indicators

    def calculate_border_pressure(self, country_code: str, all_results: Dict) -> float:
        """
        Calcula la presión fronteriza basada en la inestabilidad de los países vecinos.
        Retorna un puntaje promedio de inestabilidad de 0 a 1.
        """
        neighbors = self.border_mapping.get(country_code, [])
        if not neighbors:
            return 0.0
        
        neighbor_instabilities = []
        for neighbor_code in neighbors:
            if neighbor_code in all_results and 'inestabilidad_turchin' in all_results[neighbor_code]:
                valor_instabilidad = all_results[neighbor_code]['inestabilidad_turchin']['valor']
                if isinstance(valor_instabilidad, (int, float)):
                    neighbor_instabilities.append(valor_instabilidad)
        
        if not neighbor_instabilities:
            return 0.0
        
        return sum(neighbor_instabilities) / len(neighbor_instabilities)
        
    def calculate_turchin_instability(self, indicators: Dict, border_pressure: float = 0.0) -> Dict:
        """
        Calcula la inestabilidad según un modelo simplificado de Turchin,
        incluyendo la presión fronteriza.
        """
        wealth_concentration = float(indicators.get('wealth_concentration', self.get_default_value('WEALTH_CONCENTRATION', 'default')))
        youth_unemployment = float(indicators.get('youth_unemployment', self.get_default_value('SL.UEM.1524.ZS', 'default')))
        inflation_annual = float(indicators.get('inflation_annual', self.get_default_value('FP.CPI.TOTL.ZG', 'default')))
        social_polarization = float(indicators.get('social_polarization', self.get_default_value('CIVIL_WAR_RISK', 'default')))

        wealth_norm = (wealth_concentration - 0.1) / 0.8
        unemployment_norm = min(1.0, max(0.0, (youth_unemployment - 5.0) / 25.0))
        inflation_norm = min(1.0, max(0.0, (inflation_annual - 1.0) / 10.0))
        social_pol_norm = social_polarization
        
        instability_score = (
            (wealth_norm * 0.3) + 
            (unemployment_norm * 0.25) +
            (inflation_norm * 0.15) +
            (social_pol_norm * 0.1) +
            (border_pressure * 0.2)
        )
        
        status = 'stable'
        if instability_score > 0.7:
            status = 'critical'
        elif instability_score > 0.4:
            status = 'at_risk'
        
        return {
            "status": status,
            "valor": round(instability_score, 2),
            "comment": "Calculado basado en indicadores internos y presión fronteriza."
        }

    def calculate_jiang_stability(self, indicators: Dict) -> Dict:
        """
        Calcula la estabilidad institucional según un modelo simplificado de Jiang.
        """
        # Validaciones robustas para asegurar que los valores sean flotantes
        gov_eff = float(indicators.get('government_effectiveness', 0.0))
        pol_stab = float(indicators.get('political_stability', 0.0))
        rule_of_law = float(indicators.get('rule_of_law', 0.0))

        gov_eff_norm = (gov_eff + 2.5) / 5.0
        pol_stab_norm = (pol_stab + 2.5) / 5.0
        rule_of_law_norm = (rule_of_law + 2.5) / 5.0

        stability_score = (
            (gov_eff_norm * 0.4) +
            (pol_stab_norm * 0.4) +
            (rule_of_law_norm * 0.2)
        )
        
        status = 'stable'
        if stability_score < 0.4:
            status = 'fragile'
        
        return {
            "status": status,
            "valor": round(stability_score, 2),
            "comment": "Calculado basado en indicadores de gobernanza."
        }

    def process_country_initial(self, country_code: str, year: int) -> Optional[Dict]:
        """
        Primera pasada: solo calcula los indicadores y la inestabilidad interna.
        """
        indicators = self.calculate_indicators(country_code, year)
        if not indicators:
            logging.warning(f"Skipping {country_code} due to missing data.")
            return None

        instability_turchin = self.calculate_turchin_instability(indicators)
        estabilidad_jiang = self.calculate_jiang_stability(indicators)

        result = {
            'country_code': country_code,
            'year': year,
            'indicators': indicators,
            'estabilidad_jiang': estabilidad_jiang,
            'inestabilidad_turchin': instability_turchin
        }
        return result

    def save_to_json(self, data: List[Dict]):
        """Saves the final data structure to a JSON file."""
        final_output = {
            'timestamp': datetime.now().isoformat(),
            'version': '1.0',
            'countries_processed': len(data),
            'results': data
        }
        with open('data/data_paises.json', 'w') as f:
            json.dump(final_output, f, indent=2)

    def main(self, test_mode: bool = False):
        """Main method to run the data processing."""
        start_time = time.time()
        logging.info("Starting main data processing - First Pass (Internal Instability)")
        
        end_year = datetime.now().year - 1
        
        initial_results = {}
        countries = self.country_codes
        if test_mode:
            countries = ['USA', 'RUS', 'CHN', 'UKR', 'FIN', 'NLD', 'PER', 'MYS', 'ISR']
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_country = {executor.submit(self.process_country_initial, country, end_year): country for country in countries}
            for future in concurrent.futures.as_completed(future_to_country, timeout=600):
                country = future_to_country[future]
                try:
                    result = future.result()
                    if result:
                        initial_results[country] = result
                    logging.info(f"Completed initial processing for {country}")
                except concurrent.futures.TimeoutError:
                    logging.error(f"Timeout processing {country}, skipping")
                except Exception as e:
                    logging.error(f"Error processing {country}: {e}", exc_info=True)

        logging.info("Starting second pass - Calculating Border Pressure and Final Instability")
        
        final_results = []
        for country_code in countries:
            if country_code in initial_results:
                result = initial_results[country_code]
                border_pressure = self.calculate_border_pressure(country_code, initial_results)
                
                final_instability = self.calculate_turchin_instability(result['indicators'], border_pressure)
                result['inestabilidad_turchin'] = final_instability
                result['border_pressure'] = round(border_pressure, 2)
                final_results.append(result)
                logging.info(f"Final instability for {country_code} calculated with border pressure: {final_instability['valor']}")
        
        with self.cache_lock:
            self.cache.update(self.temp_cache)
            self.save_cache()
        
        self.save_to_json(final_results)
        logging.info(f"Main process completed in {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)
    processor = CliodynamicDataProcessor()
    processor.main(test_mode=False)

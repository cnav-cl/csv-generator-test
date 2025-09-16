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
            'regulatory_quality': 'RQ.EST',
            'happiness_score': 'WHR.SCORE'
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
            'WHR.SCORE': {'default': 5.0} # Valor por defecto para la felicidad
        }
        self.gdelt_indicators = {
            'social_polarization': 'CIVIL_WAR_RISK',
            'institutional_distrust': 'GOV_DISTRUST',
            'suicide_rate': 'SUICIDE',
            'elite_overproduction': 'ELITE_OVERPRODUCTION',
            'wealth_concentration': 'WEALTH_CONCENTRATION'
        }
        
        self.indicator_frequencies = {
            'gini_coefficient': 'anual',
            'youth_unemployment': 'anual',
            'inflation_annual': 'anual',
            'neet_ratio': 'anual',
            'tertiary_education': 'anual',
            'government_effectiveness': 'anual',
            'political_stability': 'anual',
            'control_of_corruption': 'anual',
            'voice_accountability': 'anual',
            'rule_of_law': 'anual',
            'regulatory_quality': 'anual',
            'happiness_score': 'anual',
            'social_polarization': 'semanal',
            'institutional_distrust': 'semanal',
            'suicide_rate': 'semanal',
            'elite_overproduction': 'semanal',
            'wealth_concentration': 'semanal',
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
        # Mapeo de países a los códigos de GDELT
        self.gdelt_country_codes = {
            'USA': 'US', 'CHN': 'CH', 'RUS': 'RS', 'BRA': 'BR', 'GBR': 'UK', 'FRA': 'FR', 'DEU': 'GM', 'JPN': 'JA',
            'IND': 'IN', 'CAN': 'CA', 'MEX': 'MX', 'AUS': 'AS', 'KOR': 'KR', 'ITA': 'IT', 'SAU': 'SA', 'TUR': 'TU',
            'EGY': 'EG', 'NGA': 'NI', 'PAK': 'PK', 'IDN': 'ID', 'VNM': 'VM', 'PHL': 'RP', 'ARG': 'AR', 'COL': 'CO',
            'POL': 'PL', 'ESP': 'SP', 'SP': 'SP', 'IRN': 'IR', 'ZAF': 'SF', 'UKR': 'UP', 'THA': 'TH', 'VEN': 'VE', 'CHL': 'CI',
            'PER': 'PE', 'MYS': 'MY', 'ROU': 'RO', 'SWE': 'SW', 'BEL': 'BE', 'NLD': 'NL', 'GRC': 'GR', 'CZE': 'EZ',
            'PRT': 'PO', 'DNK': 'DA', 'FIN': 'FI', 'NO': 'NO', 'SGP': 'SN', 'AUT': 'AU', 'CHE': 'SZ', 'IRL': 'EI',
            'NZL': 'NZ', 'HKG': 'HK', 'ISR': 'IS', 'ARE': 'AE', 'UKR': 'UP'
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

    def _should_refresh(self, cache_entry: Dict, frequency: str) -> bool:
        """Determines if a cached value needs to be refreshed based on its frequency."""
        if not cache_entry:
            return True
        
        last_retrieved_date_str = cache_entry.get('retrieved_on')
        if not last_retrieved_date_str:
            return True
        
        last_retrieved_date = datetime.strptime(last_retrieved_date_str, '%Y-%m-%d')
        now = datetime.now()

        if frequency == 'anual':
            return now.year > last_retrieved_date.year
        elif frequency == 'trimestral':
            # Check if a month has passed
            return (now - last_retrieved_date).days >= 30
        elif frequency == 'semanal':
            # Check if a week has passed
            return (now - last_retrieved_date).days >= 7
        
        return True # Default to refresh if frequency is unknown

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
    
    def fetch_imf_data(self, country_code: str, indicator_code: str, start_year: int, end_year: int) -> Dict:
        """
        Fetches historical data from the IMF API for a given indicator within a year range.
        Returns a dictionary mapping year to value.
        """
        api_url = f"https://www.imf.org/external/datamapper/api/v1/{indicator_code}/{country_code}?periods={start_year}:{end_year}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        historical_data = {}

        for i in range(3):
            try:
                response = requests.get(api_url, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                series_data = data.get('values', {}).get(indicator_code, {}).get(country_code, {})
                if series_data:
                    for year_str, value in series_data.items():
                        if value is not None:
                            historical_data[int(year_str)] = float(value)
                    logging.info(f"✅ Found historical data for {indicator_code} from IMF.")
                    break
            except (requests.exceptions.RequestException, json.JSONDecodeError, IndexError, TypeError) as e:
                logging.warning(f"❌ Attempt {i+1} failed for {indicator_code} in {country_code} from IMF: {e}")
                if i < 2:
                    time.sleep(2 ** i)
        return historical_data

    def fetch_world_bank_data(self, country_code: str, indicator_code: str, start_year: int, end_year: int) -> Dict:
        """
        Fetches World Bank data, iterating backward to find the most recent valid value
        and then projects it to the current year if needed. Includes retries.
        """
        headers = {'User-Agent': 'Mozilla/5.0'}
        historical_data = {}

        for year in range(end_year, start_year - 1, -1):
            api_url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}?date={year}&format=json"
            
            for i in range(3):
                try:
                    response = requests.get(api_url, headers=headers, timeout=30) 
                    response.raise_for_status()
                    data = response.json()
                    
                    if len(data) > 1 and data[1] and data[1][0]['value'] is not None:
                        historical_data[int(data[1][0]['date'])] = data[1][0]['value']
                        break
                except (requests.exceptions.RequestException, json.JSONDecodeError, IndexError, TypeError) as e:
                    logging.warning(f"❌ Attempt {i+1} failed for {indicator_code} in {country_code} for year {year}: {e}")
                    if i < 2:
                        time.sleep(2 ** i)
                    else:
                        logging.warning(f"Max retries reached for {indicator_code} in {country_code}. Trying previous year.")
        return historical_data

    def _fetch_happiness_data(self, country_code: str) -> Optional[float]:
        """
        Simula la obtención de datos del World Happiness Report.
        En una implementación real, esto podría parsear un CSV o usar una API.
        """
        # Datos de ejemplo basados en el World Happiness Report 2024
        happiness_data = {
            'FIN': 7.74, 'DNK': 7.58, 'ISL': 7.53, 'SWE': 7.34, 'ISR': 7.34, 'NLD': 7.32,
            'NOR': 7.30, 'LUX': 7.12, 'CHE': 7.06, 'AUS': 7.05, 'NZL': 7.02, 'USA': 6.89,
            'DEU': 6.78, 'GBR': 6.74, 'CAN': 6.64, 'IRL': 6.55, 'BEL': 6.54, 'CZE': 6.50,
            'KOR': 6.05, 'MEX': 6.02, 'BRA': 6.00, 'CHL': 5.96, 'ARG': 5.86, 'JPN': 5.84,
            'IDN': 5.58, 'RUS': 5.66, 'ESP': 6.42, 'ITA': 6.26, 'FRA': 6.60
        }
        
        score = happiness_data.get(country_code)
        if score:
            logging.info(f"✅ Happiness score fetched for {country_code}: {score:.2f}")
        else:
            score = self.get_default_value('WHR.SCORE', country_code)
            logging.warning(f"⚠️ No happiness data found for {country_code}. Using default: {score:.2f}")
        return score
        
    def fetch_gdelt_indicator(self, country_code: str, indicator_name: str) -> float:
        """
        Calcula un indicador de GDELT basándose en la frecuencia de temas y eventos.
        Retorna un valor normalizado entre 0.0 y 1.0, basado en un promedio mensual.
        """
        gdelt_queries = {
            'social_polarization': 'protest OR riot OR "social unrest" OR "political tension"',
            'institutional_distrust': '"government corruption" OR "political scandal" OR "institutional failure" OR "public distrust"',
            'suicide_rate': '"suicide" OR "suicide rate"',
            'elite_overproduction': '"elite overproduction" OR "elite competition" OR "political infighting"',
            'wealth_concentration': '"wealth inequality" OR "gini coefficient" OR "income gap" OR "billionaires" theme:WB_1603'
        }
        
        country_gdelt_code = self.gdelt_country_codes.get(country_code, None)
        if not country_gdelt_code:
            logging.warning(f"❌ GDELT country code not found for {country_code}. Using default.")
            return random.uniform(0.1, 0.9)
            
        query_string = gdelt_queries.get(indicator_name)
        if not query_string:
            logging.warning(f"❌ GDELT query not found for indicator: {indicator_name}. Using default.")
            return random.uniform(0.1, 0.9)
            
        try:
            # Petición para obtener datos de los últimos 30 días con granularidad mensual
            api_url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={query_string}&mode=TimelineVol&country={country_gdelt_code}&format=json&timespan=30days&timezoom=yes"
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            timeline = data.get('timeline', [])
            if timeline:
                # Tomar los últimos 5 puntos de datos para calcular un promedio y suavizar el ruido diario
                last_values = [d.get('value', 0) for d in timeline[-5:]]
                
                if last_values:
                    recent_average = sum(last_values) / len(last_values)
                else:
                    recent_average = 0
                
                # Normalizar el valor promedio a un rango de 0.0 a 1.0. Los valores aquí son una estimación
                # y deben ajustarse con la calibración. Se usan valores más pequeños que el resumen anual.
                if 'social_polarization' in indicator_name or 'distrust' in indicator_name:
                    normalized_value = min(1.0, recent_average / 50.0)
                elif 'elite_overproduction' in indicator_name or 'wealth_concentration' in indicator_name:
                    normalized_value = min(1.0, recent_average / 100.0)
                else:
                    normalized_value = min(1.0, recent_average / 10.0)
                    
                logging.info(f"✅ GDELT data fetched for {indicator_name} in {country_code}: {normalized_value:.2f} (from a 30-day average)")
                return normalized_value
                
        except (requests.exceptions.RequestException, json.JSONDecodeError, IndexError, TypeError) as e:
            logging.error(f"❌ Error fetching GDELT data for {indicator_name} in {country_code}: {e}")
            
        return random.uniform(0.1, 0.9)

    def calculate_indicators(self, country_code: str, year: int) -> Dict:
        """Calculates indicators, ensuring values are never None."""
        indicators = {}
        end_year = self.current_year
        
        # Procesar indicadores anuales y trimestrales
        for name, wb_code in self.indicators.items():
            cache_key = f"{country_code}_{name}"
            frequency = self.indicator_frequencies.get(name, 'anual')
            
            # Verificar si se debe refrescar el caché
            with self.cache_lock:
                cache_entry = self.cache.get(cache_key)
                should_refresh = self._should_refresh(cache_entry, frequency)
            
            if not should_refresh:
                indicators[name] = float(cache_entry['value'])
                logging.info(f"✅ Usando valor en caché para {name} ({country_code}) del año {cache_entry['year']}.")
                continue
            
            # Si el caché no es válido, buscar nuevos datos
            final_value = None
            most_recent_year = None
            
            if name == 'happiness_score':
                final_value = self._fetch_happiness_data(country_code)
                most_recent_year = end_year
            else:
                historical_data = {}
                wb_data = self.fetch_world_bank_data(country_code, wb_code, end_year - 5, end_year)
                historical_data.update(wb_data)
                
                if name in self.imf_indicators:
                    imf_code = self.imf_indicators[name]
                    imf_data = self.fetch_imf_data(country_code, imf_code, end_year - 5, end_year)
                    historical_data.update(imf_data)
                
                if historical_data:
                    df = pd.DataFrame(historical_data.items(), columns=['year', 'value']).sort_values('year').drop_duplicates(subset=['year'], keep='last')
                    
                    most_recent_year = df['year'].max()
                    most_recent_value = df.loc[df['year'] == most_recent_year, 'value'].iloc[0]

                    if most_recent_year >= end_year - 1:
                        final_value = most_recent_value
                        logging.info(f"✅ Encontrado dato reciente para {name} ({country_code}) del año {most_recent_year}.")
                    elif len(df) >= 2:
                        try:
                            df['year'] = pd.to_datetime(df['year'], format='%Y').dt.to_period('Y')
                            df = df.set_index('year')
                            
                            fit = SimpleExpSmoothing(df['value'], initialization_method="estimated_slinear").fit()
                            forecast = fit.forecast(1).iloc[0]
                            final_value = float(forecast)
                            logging.info(f"🔄 Proyectando {name} para {country_code} de {most_recent_year} a {end_year} usando datos combinados: {round(final_value, 2)}")
                        except Exception as e:
                            logging.error(f"❌ Fallo al proyectar para {name} en {country_code}: {e}. Usando valor más reciente.")
                            final_value = most_recent_value
                    else:
                        final_value = most_recent_value
                        logging.warning(f"⚠️ No hay suficientes puntos de datos para {name} en {country_code} para una proyección. Usando el valor más reciente: {final_value}")
            
            if final_value is None:
                final_value = self.get_default_value(wb_code, country_code)
                logging.warning(f"⚠️ No se encontró dato válido para {name} en {country_code}. Usando valor por defecto: {final_value}")
            
            indicators[name] = float(final_value)

            # Almacenar el resultado en el caché temporal
            with self.cache_lock:
                self.temp_cache[cache_key] = {
                    'value': indicators[name],
                    'year': most_recent_year if most_recent_year is not None else end_year,
                    'retrieved_on': str(datetime.now().date())
                }

        # Procesar indicadores de GDELT (alta frecuencia)
        for name in self.gdelt_indicators:
            cache_key = f"{country_code}_gdelt_{name}"
            frequency = self.indicator_frequencies.get(name, 'semanal')
            
            with self.cache_lock:
                cache_entry = self.cache.get(cache_key)
                should_refresh = self._should_refresh(cache_entry, frequency)
            
            if not should_refresh:
                indicators[name] = float(cache_entry['value'])
                logging.info(f"✅ Usando valor en caché para {name} ({country_code}) de GDELT.")
            else:
                indicators[name] = self.fetch_gdelt_indicator(country_code, name)
                with self.cache_lock:
                    self.temp_cache[cache_key] = {
                        'value': indicators[name],
                        'year': end_year, # No aplica, pero se guarda para consistencia
                        'retrieved_on': str(datetime.now().date())
                    }

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
        incluyendo la presión fronteriza y el factor de felicidad.
        """
        wealth_concentration = float(indicators.get('wealth_concentration', self.get_default_value('WEALTH_CONCENTRATION', 'default')))
        youth_unemployment = float(indicators.get('youth_unemployment', self.get_default_value('SL.UEM.1524.ZS', 'default')))
        inflation_annual = float(indicators.get('inflation_annual', self.get_default_value('FP.CPI.TOTL.ZG', 'default')))
        social_polarization = float(indicators.get('social_polarization', self.get_default_value('CIVIL_WAR_RISK', 'default')))
        happiness_score = float(indicators.get('happiness_score', self.get_default_value('WHR.SCORE', 'default')))

        wealth_norm = (wealth_concentration - 0.1) / 0.8
        unemployment_norm = min(1.0, max(0.0, (youth_unemployment - 5.0) / 25.0))
        inflation_norm = min(1.0, max(0.0, (inflation_annual - 1.0) / 10.0))
        social_pol_norm = social_polarization
        
        # El puntaje de felicidad se normaliza y actúa como un factor de reducción de la inestabilidad.
        # Los puntajes de felicidad van de ~2.5 a ~7.8. Normalizamos a un rango de 0 a 1.
        happiness_norm = min(1.0, max(0.0, (happiness_score - 2.5) / 5.3))

        instability_score = (
            (wealth_norm * 0.3) + 
            (unemployment_norm * 0.25) +
            (inflation_norm * 0.15) +
            (social_pol_norm * 0.1) +
            (border_pressure * 0.2)
        )
        
        # Reducción de la inestabilidad por la felicidad
        instability_score = instability_score - (happiness_norm * 0.15)
        
        # Asegurarse de que el puntaje no sea negativo
        instability_score = max(0.0, instability_score)
        
        status = 'stable'
        if instability_score > 0.7:
            status = 'critical'
        elif instability_score > 0.4:
            status = 'at_risk'
        
        return {
            "status": status,
            "valor": round(instability_score, 2),
            "comment": "Calculado basado en indicadores internos, presión fronteriza y un factor de felicidad."
        }

    def calculate_jiang_stability(self, indicators: Dict) -> Dict:
        """
        Calcula la estabilidad institucional según un modelo simplificado de Jiang.
        """
        gov_eff = float(indicators.get('government_effectiveness', self.get_default_value('GE.EST', 'default')))
        pol_stab = float(indicators.get('political_stability', self.get_default_value('PV.EST', 'default')))
        rule_of_law = float(indicators.get('rule_of_law', self.get_default_value('RL.EST', 'default')))

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
        
        end_year = self.current_year
        
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

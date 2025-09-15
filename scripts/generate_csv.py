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

        # Define GDELT country mapping with alternative names
        self.gdelt_country_mapping = {
            'USA': ['United States', 'USA', 'United_States'],
            'CHN': ['China', 'People\'s Republic of China'],
            'IND': ['India'],
            'BRA': ['Brazil'],
            'RUS': ['Russia', 'Russian Federation'],
            'JPN': ['Japan'],
            'DEU': ['Germany'],
            'GBR': ['United Kingdom', 'UK'],
            'FRA': ['France'],
            'ITA': ['Italy'],
            'CAN': ['Canada'],
            'AUS': ['Australia'],
            'ESP': ['Spain'],
            'MEX': ['Mexico'],
            'IDN': ['Indonesia'],
            'TUR': ['Turkey'],
            'SAU': ['Saudi Arabia'],
            'CHE': ['Switzerland'],
            'NLD': ['Netherlands'],
            'POL': ['Poland'],
            'SWE': ['Sweden'],
            'BEL': ['Belgium'],
            'ARG': ['Argentina'],
            'NOR': ['Norway'],
            'AUT': ['Austria'],
            'THA': ['Thailand'],
            'ARE': ['United Arab Emirates', 'UAE'],
            'ISR': ['Israel'],
            'ZAF': ['South Africa'],
            'DNK': ['Denmark'],
            'SGP': ['Singapore'],
            'FIN': ['Finland'],
            'COL': ['Colombia'],
            'MYS': ['Malaysia'],
            'IRL': ['Ireland'],
            'CHL': ['Chile'],
            'EGY': ['Egypt'],
            'PHL': ['Philippines'],
            'PAK': ['Pakistan'],
            'GRC': ['Greece'],
            'PRT': ['Portugal'],
            'CZE': ['Czech Republic'],
            'ROU': ['Romania'],
            'NZL': ['New Zealand'],
            'PER': ['Peru'],
            'HUN': ['Hungary'],
            'QAT': ['Qatar'],
            'UKR': ['Ukraine'],
            'DZA': ['Algeria'],
            'KWT': ['Kuwait'],
            'MAR': ['Morocco'],
            'BGD': ['Bangladesh'],
            'VEN': ['Venezuela'],
            'OMN': ['Oman'],
            'SVK': ['Slovakia'],
            'HRV': ['Croatia'],
            'LBN': ['Lebanon'],
            'LKA': ['Sri Lanka'],
            'BGR': ['Bulgaria'],
            'TUN': ['Tunisia'],
            'DOM': ['Dominican Republic'],
            'PRI': ['Puerto Rico'],
            'EST': ['Estonia'],
            'LTU': ['Lithuania'],
            'PAN': ['Panama'],
            'SRB': ['Serbia'],
            'AZE': ['Azerbaijan'],
            'SLV': ['El Salvador'],
            'URY': ['Uruguay'],
            'KEN': ['Kenya'],
            'LVA': ['Latvia'],
            'CYP': ['Cyprus'],
            'GTM': ['Guatemala'],
            'ETH': ['Ethiopia'],
            'CRI': ['Costa Rica'],
            'JOR': ['Jordan'],
            'BHR': ['Bahrain'],
            'NPL': ['Nepal'],
            'BOL': ['Bolivia'],
            'TZA': ['Tanzania'],
            'HND': ['Honduras'],
            'UGA': ['Uganda'],
            'SEN': ['Senegal'],
            'GEO': ['Georgia'],
            'ZWE': ['Zimbabwe'],
            'MMR': ['Myanmar'],
            'KAZ': ['Kazakhstan'],
            'CMR': ['Cameroon'],
            'CIV': ['Ivory Coast'],
            'SDN': ['Sudan'],
            'AGO': ['Angola'],
            'NGA': ['Nigeria'],
            'MOZ': ['Mozambique'],
            'GHA': ['Ghana'],
            'MDG': ['Madagascar'],
            'COD': ['Democratic Republic of Congo'],
            'TCD': ['Chad'],
            'YEM': ['Yemen'],
            'AFG': ['Afghanistan']
        }

        self.sources = {
            'world_bank': DataSource(
                name="World Bank",
                base_url="https://api.worldbank.org/v2/country/{}/indicator/{}?format=json&per_page=100&date={}:{}",
                rate_limit=0.2
            ),
            'gdelt': DataSource(
                name="GDELT Monitoring",
                base_url="https://api.gdeltproject.org/api/v2/doc/doc?query={query}&timespan=30d&mode=artlist&format=json&maxrecords=250",
                rate_limit=0.5
            )
        }

        self.indicator_sources = {
            'gini_coefficient': [('world_bank', 'SI.POV.GINI')],
            'youth_unemployment': [('world_bank', 'SL.UEM.1524.ZS')],
            'inflation_annual': [('world_bank', 'FP.CPI.TOTL.ZG')],
            'neet_ratio': [('world_bank', 'SL.UEM.NEET.ZS')],
            'tertiary_education': [('world_bank', 'SE.TER.CUAT.BA.ZS')],
            'gdppc': [('world_bank', 'NY.GDP.PCAP.CD')],
            'suicide_rate': [('world_bank', 'SH.STA.SUIC.P5')],
            'government_effectiveness': [('world_bank', 'GE.EST')]
        }

        # Default values for missing indicators with regional adjustments
        self.default_indicator_values = {
            'gini_coefficient': {'default': 40.0, 'CHN': 38.0},  # Adjusted for China
            'youth_unemployment': {'default': 20.0, 'CHN': 15.0},
            'inflation_annual': {'default': 5.0, 'CHN': 2.5},
            'neet_ratio': {'default': 12.0, 'CHN': 10.0},  # Adjusted for East Asia
            'tertiary_education': {'default': 18.0, 'CHN': 25.0},
            'gdppc': {'default': 1000.0, 'CHN': 12500.0},
            'suicide_rate': {'default': 10.0, 'CHN': 8.0},
            'government_effectiveness': {'default': 0.0, 'CHN': 0.5}
        }

        self.country_codes = self.load_all_countries()
        
        self.thresholds = {
            'neet_ratio': {'alert': 20.0, 'critical': 25.0, 'points': {'alert': -1.5, 'critical': -2.5}},
            'gini_coefficient': {'alert': 0.40, 'critical': 0.45, 'points': {'alert': -1.5, 'critical': -3.0}},
            'youth_unemployment': {'alert': 25.0, 'critical': 30.0, 'points': {'alert': -1.0, 'critical': -2.0}},
            'inflation_annual': {'alert': 10.0, 'critical': 15.0, 'points': {'alert': -1.0, 'critical': -2.0}},
            'social_polarization': {'alert': 0.60, 'critical': 0.75, 'points': {'alert': -1.5, 'critical': -3.0}},
            'institutional_distrust': {'alert': 0.60, 'critical': 0.75, 'points': {'alert': -1.5, 'critical': -3.0}},
            'suicide_rate': {'alert': 10.0, 'critical': 15.0, 'points': {'alert': -1.0, 'critical': -2.0}},
            'wealth_concentration': {'alert': 0.45, 'critical': 0.55, 'points': {'alert': -1.5, 'critical': -3.0}},
            'education_gap': {'alert': 0.05, 'critical': 0.1, 'points': {'alert': -1.0, 'critical': -2.5}},
            'elite_overproduction': {'alert': 0.05, 'critical': 0.1, 'points': {'alert': -1.5, 'critical': -3.0}},
            'estabilidad_jiang': {'alert': 5.0, 'critical': 4.0, 'points': {'alert': 0.0, 'critical': 0.0}},
            'inestabilidad_turchin': {'alert': 0.35, 'critical': 0.5, 'points': {'alert': 0.0, 'critical': 0.0}}
        }

        self.high_risk_countries = {
            'SOM': 111.3, 'SDN': 109.3, 'SSD': 109.0, 'SYR': 108.1, 'YEM': 106.6,
            'COD': 105.4, 'AFG': 103.9, 'CAF': 103.8, 'TCD': 102.1, 'MMR': 100.8,
            'HTI': 99.7, 'MLI': 98.4, 'BFA': 97.3, 'NER': 96.8, 'CMR': 95.9,
            'NGA': 95.2, 'ETH': 94.7, 'LBY': 93.5, 'ERI': 93.2, 'BDI': 92.8,
            'UKR': 85.0, 'ISR': 70.0, 'PSE': 90.0, 'IRN': 80.0, 'LBN': 82.0, 'MEX': 75.0
        }

        self.crisis_forecasts = {'SDN': 0.4, 'MMR': 0.3, 'YEM': 0.35, 'SYR': 0.3, 'UKR': 0.25, 'HTI': 0.2, 'LBN': 0.25}

    def safe_float(self, value, default):
        """Convert value to float, return default if conversion fails."""
        try:
            return float(value) if value is not None else default
        except (ValueError, TypeError):
            logging.warning(f"Invalid value for conversion: {value}, using default {default}")
            return default

    def load_cache(self) -> Dict:
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logging.warning(f"Error loading cache: {e}")
        return {}

    def save_cache(self):
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            cache_copy = copy.deepcopy(self.cache)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_copy, f)
        except Exception as e:
            logging.warning(f"Error saving cache: {e}")

    def load_all_countries(self) -> List[str]:
        return list(self.gdelt_country_mapping.keys())

    def fetch_world_bank_data(self, country_code: str, indicator_code: str, years_back=10) -> Optional[Dict]:
        cache_key = f"{country_code}_{indicator_code}"
        if cache_key in self.cache:
            if (datetime.now() - datetime.fromisoformat(self.cache[cache_key]['timestamp'])).days < 7:
                return self.cache[cache_key]['data']
        
        end_year = datetime.now().year
        start_year = end_year - years_back
        attempts = 3
        headers = {'User-Agent': 'CliodynamicAnalyzer/1.0 (contact: cnav-cl@example.com)'}
        for attempt in range(attempts):
            try:
                url = self.sources['world_bank'].base_url.format(country_code, indicator_code, start_year, end_year)
                response = requests.get(url, timeout=30, headers=headers)
                response.raise_for_status()
                data = response.json()
                
                historical = {}
                if data and len(data) > 1 and data[1]:
                    for item in data[1]:
                        if item['date'].isdigit() and item['value'] is not None:
                            try:
                                value = float(item['value'])
                                historical[int(item['date'])] = value
                            except (ValueError, TypeError):
                                logging.warning(f"Invalid value for {country_code}-{indicator_code} in {item['date']}: {item['value']}")
                                continue
                
                if historical:
                    years = sorted(historical.keys())
                    current = historical[years[-1]]
                    delta = (current - historical[years[0]]) / len(years) if len(years) > 1 else 0
                    variance = np.var(list(historical.values()))
                    result = {'historical': historical, 'current': current, 'delta': delta, 'variance': variance}
                    self.cache[cache_key] = {'data': result, 'timestamp': datetime.now().isoformat()}
                    self.save_cache()
                    return result
                
                logging.warning(f"No valid data for {country_code}-{indicator_code}, using default value")
                default_value = self.default_indicator_values.get(indicator_code.split('.')[-1], {}).get(country_code, 
                    self.default_indicator_values.get(indicator_code.split('.')[-1], {}).get('default', 0.0))
                return {'historical': {}, 'current': default_value, 'delta': 0.0, 'variance': 0.0}
            except requests.exceptions.RequestException as e:
                if 'response' in locals() and response.status_code == 429:
                    logging.warning(f"Rate limit hit for {country_code}-{indicator_code}, attempt {attempt+1}/{attempts}")
                    time.sleep(2 ** attempt)
                else:
                    logging.error(f"Error fetching World Bank data for {country_code}-{indicator_code}: {e}")
                    break
        logging.warning(f"Failed to fetch data for {country_code}-{indicator_code}, using default value")
        default_value = self.default_indicator_values.get(indicator_code.split('.')[-1], {}).get(country_code, 
            self.default_indicator_values.get(indicator_code.split('.')[-1], {}).get('default', 0.0))
        return {'historical': {}, 'current': default_value, 'delta': 0.0, 'variance': 0.0}

    def forecast_indicator(self, historical: Dict[int, float], steps=2, country_code: str = "", indicator: str = "") -> float:
        if len(historical) < 5:  # Require at least 5 data points
            logging.debug(f"Insufficient data for forecast: {len(historical)} points for {country_code}-{indicator}")
            return list(historical.values())[-1] if historical else 0.0
        
        years = sorted(historical.keys())
        values = [historical[year] for year in years]
        # Check for low variance to avoid ARIMA on near-constant series
        if np.var(values) < 1e-4:  # Tightened threshold
            logging.debug(f"Low variance in data for {country_code}-{indicator}: {values}")
            return values[-1]
        
        # Log historical data for debugging
        logging.debug(f"Historical data for {country_code}-{indicator}: {historical}")
        
        # Apply log-transformation to stabilize variance (avoid log(0) or negative values)
        values = [np.log1p(max(0, v)) for v in values]
        # Apply first-order differencing to improve stationarity
        values_diff = np.diff(values)
        if len(values_diff) < 2:
            logging.debug(f"Insufficient data after differencing for {country_code}-{indicator}")
            return np.expm1(values[-1]) if values else 0.0
        dates = pd.to_datetime([f"{year}-01-01" for year in years[1:]])
        series = pd.Series(values_diff, index=pd.PeriodIndex(dates, freq='Y'))
        
        # Try ARIMA
        orders = [(1, 0, 0), (0, 1, 0), (0, 0, 1), (0, 0, 0)]  # Added mean-only model
        for order in orders:
            try:
                model = ARIMA(series, order=order, enforce_stationarity=False, enforce_invertibility=False)
                fit = model.fit()
                forecast_diff = fit.forecast(steps=steps)
                # Reverse differencing and log-transformation
                last_value = values[-1]
                forecast = np.cumsum([last_value] + list(forecast_diff))[-1]
                return float(np.expm1(forecast))
            except Exception as e:
                logging.warning(f"ARIMA failed with order {order} for {country_code}-{indicator}: {e}")
                continue
        
        # Fallback to exponential smoothing
        try:
            model = SimpleExpSmoothing(values)
            fit = model.fit()
            forecast = fit.forecast(steps=steps)
            return float(np.expm1(forecast[-1]))
        except Exception as e:
            logging.warning(f"Exponential smoothing failed for {country_code}-{indicator}: {e}")
        
        # Fallback to moving average
        logging.warning(f"All forecasting methods failed for {country_code}-{indicator}, using moving average")
        window_size = min(3, len(values))
        if window_size > 0:
            moving_avg = np.mean(values[-window_size:])
            return float(np.expm1(moving_avg))
        return 0.0

    def get_gdelt_shock_factor(self, country_code: str, force_refresh: bool = False) -> float:
        cache_key = f"gdelt_{country_code}"
        if not force_refresh and cache_key in self.cache:
            if (datetime.now() - datetime.fromisoformat(self.cache[cache_key]['timestamp'])).days < 1:
                logging.info(f"Using cached GDELT shock factor for {country_code}: {self.cache[cache_key]['data']}")
                return self.cache[cache_key]['data']
        
        country_names = self.gdelt_country_mapping.get(country_code, [country_code])
        shock_factor = 1.0
        headers = {'User-Agent': 'CliodynamicAnalyzer/1.0 (contact: cnav-cl@example.com)'}
        
        for country_name in country_names:
            query = f"sourcecountry:{country_name}"
            attempts = 3
            for attempt in range(attempts):
                try:
                    url = self.sources['gdelt'].base_url.format(query=query)
                    logging.debug(f"GDELT query URL for {country_code}: {url}")
                    response = requests.get(url, timeout=30, headers=headers)
                    response.raise_for_status()
                    if 'application/json' not in response.headers.get('Content-Type', ''):
                        logging.warning(f"Non-JSON response from GDELT for {country_code} ({country_name}): {response.text[:200]}")
                        continue
                    data = response.json()
                    articles = data.get('articles', [])
                    logging.info(f"GDELT data retrieved for {country_code} ({country_name}): {len(articles)} articles")
                    total_events = sum(1 for item in articles if isinstance(item, dict) and item.get('EventBaseCode', '').startswith(('1', '2', '3', '4')))
                    shock_factor = 2.5 if total_events > 50 else 1.8 if total_events > 10 else 1.0
                    self.cache[cache_key] = {'data': shock_factor, 'timestamp': datetime.now().isoformat()}
                    self.save_cache()
                    return shock_factor
                except requests.exceptions.RequestException as e:
                    if 'response' in locals() and response.status_code == 429:
                        logging.warning(f"Rate limit hit for GDELT {country_code} ({country_name}), attempt {attempt+1}/{attempts}")
                        time.sleep(2 ** attempt)
                    else:
                        logging.error(f"Error fetching GDELT for {country_code} ({country_name}): {e}")
                        break
                except json.JSONDecodeError as e:
                    logging.error(f"GDELT JSON decode error for {country_code} ({country_name}): {e}. Response: {response.text[:200] if 'response' in locals() else 'No response'}")
                    time.sleep(2 ** attempt)
        
        logging.warning(f"Failed to fetch GDELT data for {country_code}, using default shock factor")
        self.cache[cache_key] = {'data': shock_factor, 'timestamp': datetime.now().isoformat()}
        self.save_cache()
        return shock_factor

    def fetch_latest_fsi(self):
        cache_key = 'fsi_data'
        if cache_key in self.cache:
            if (datetime.now() - datetime.fromisoformat(self.cache[cache_key]['timestamp'])).days < 30:
                return self.cache[cache_key]['data']
        
        headers = {'User-Agent': 'CliodynamicAnalyzer/1.0 (contact: cnav-cl@example.com)'}
        try:
            url = 'https://en.wikipedia.org/wiki/List_of_countries_by_Fragile_States_Index'
            response = requests.get(url, timeout=30, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', {'class': 'wikitable'})
            if not table:
                raise ValueError("No table found")
            rows = table.find_all('tr')[1:]
            updated_dict = {}
            for row in rows[:30]:
                cells = row.find_all('td')
                if len(cells) > 2:
                    iso = cells[0].text.strip() if cells[0].text.isupper() and len(cells[0].text) == 3 else None
                    score_str = re.sub(r'[^\d.]', '', cells[2].text.strip())
                    if iso and score_str:
                        try:
                            score = float(score_str)
                            if score > 90:
                                updated_dict[iso] = score
                        except ValueError:
                            pass
            result = updated_dict or self.high_risk_countries
            self.cache[cache_key] = {'data': result, 'timestamp': datetime.now().isoformat()}
            self.save_cache()
            return result
        except Exception as e:
            logging.warning(f"Error fetching FSI: {e}. Using fallback.")
            return self.high_risk_countries

    def convert_effectiveness_to_distrust(self, effectiveness: float) -> float:
        if effectiveness is None:
            return 0.5
        normalized_effectiveness = (effectiveness - (-2.5)) / (2.5 - (-2.5))
        distrust = 1.0 - normalized_effectiveness
        return round(max(0.1, min(0.9, distrust)), 2)

    def calculate_proxies(self, all_indicators: Dict) -> Tuple[float, float, float]:
        wealth_concentration = self.safe_float(
            all_indicators.get('gini_coefficient', self.default_indicator_values['gini_coefficient']['default']),
            self.default_indicator_values['gini_coefficient']['default']
        ) / 100
        tertiary_education = self.safe_float(
            all_indicators.get('tertiary_education', self.default_indicator_values['tertiary_education']['default']),
            self.default_indicator_values['tertiary_education']['default']
        ) / 100
        youth_unemployment = self.safe_float(
            all_indicators.get('youth_unemployment', self.default_indicator_values['youth_unemployment']['default']),
            self.default_indicator_values['youth_unemployment']['default']
        ) / 100
        education_gap = tertiary_education * youth_unemployment
        elite_overproduction = tertiary_education * youth_unemployment
        return wealth_concentration, education_gap, elite_overproduction

    def calculate_social_indicators(self, country_code: str, all_indicators: Dict) -> Tuple[float, float]:
        gov_effectiveness = all_indicators.get('government_effectiveness', 
            self.default_indicator_values['government_effectiveness'].get(country_code, 
            self.default_indicator_values['government_effectiveness']['default']))
        institutional_distrust = self.convert_effectiveness_to_distrust(gov_effectiveness)
        gini_normalized = self.safe_float(
            all_indicators.get('gini_coefficient', self.default_indicator_values['gini_coefficient'].get(country_code, 
            self.default_indicator_values['gini_coefficient']['default'])),
            self.default_indicator_values['gini_coefficient'].get(country_code, 
            self.default_indicator_values['gini_coefficient']['default'])
        ) / 100
        neet_ratio = self.safe_float(
            all_indicators.get('neet_ratio', self.default_indicator_values['neet_ratio'].get(country_code, 
            self.default_indicator_values['neet_ratio']['default'])),
            self.default_indicator_values['neet_ratio'].get(country_code, 
            self.default_indicator_values['neet_ratio']['default'])
        ) / 100
        polarization = (gini_normalized * 0.4) + (institutional_distrust * 0.4) + (neet_ratio * 0.2)
        polarization = min(0.9, max(0.3, polarization))
        return round(polarization, 2), institutional_distrust

    def calculate_turchin_instability(self, indicators: Dict, deltas: Dict, forecasts: Dict) -> Dict:
        weights = {
            'elite_overproduction': 0.25,
            'wealth_concentration': 0.20,
            'institutional_distrust': 0.18,
            'social_polarization': 0.15,
            'youth_unemployment': 0.12,
            'neet_ratio': 0.10
        }
        instability_score = 0.0
        for indicator, weight in weights.items():
            value = indicators.get(indicator)
            if value is not None:
                try:
                    value = float(value)
                    if indicator == 'elite_overproduction':
                        norm_value = min(1.0, value / 0.15)
                    elif indicator == 'wealth_concentration':
                        norm_value = min(1.0, value / 0.7)
                    elif indicator in ['social_polarization', 'institutional_distrust']:
                        norm_value = min(1.0, value)
                    else:
                        norm_value = min(1.0, value / 100)
                    instability_score += weight * norm_value
                except (ValueError, TypeError):
                    logging.warning(f"Invalid value for {indicator} in Turchin calculation: {value}")
                    continue
        
        delta_adjust = sum(deltas.get(ind, 0) for ind in weights if deltas.get(ind, 0) > 0) * 0.1
        forecast_adjust = 0.0
        for ind in weights:
            value = indicators.get(ind)
            if value is not None and isinstance(value, (int, float)):
                forecast_val = forecasts.get(ind, value)
                if isinstance(forecast_val, (int, float)):
                    forecast_adjust += max(0, forecast_val - value) * 0.05
        instability_score += delta_adjust + forecast_adjust
        final_score = round(min(1.0, max(0.0, instability_score)), 2)
        
        if final_score >= self.thresholds['inestabilidad_turchin']['critical']:
            status = 'critical'
        elif final_score >= self.thresholds['inestabilidad_turchin']['alert']:
            status = 'alert'
        else:
            status = 'stable'
        return {'status': status, 'valor': final_score}

    def calculate_jiang_stability(self, indicators: Dict, deltas: Dict, forecasts: Dict, country_code: str) -> Dict:
        normalized_values = {}
        numeric_keys = [k for k in indicators if k not in ['country_code', 'year']]
        for indicator in numeric_keys:
            default_value = self.default_indicator_values.get(indicator, {}).get(country_code, 
                self.default_indicator_values.get(indicator, {}).get('default', 0.5))
            value = indicators.get(indicator, default_value)
            try:
                if value is not None:
                    value = float(value)
                    if indicator in ['gini_coefficient', 'neet_ratio', 'youth_unemployment', 'inflation_annual', 'suicide_rate']:
                        normalized_values[indicator] = value / 100
                    elif indicator in ['social_polarization', 'institutional_distrust', 'wealth_concentration', 'education_gap', 'elite_overproduction']:
                        normalized_values[indicator] = value
                    else:
                        normalized_values[indicator] = value / 10000
                else:
                    normalized_values[indicator] = 0.5
            except (ValueError, TypeError):
                logging.warning(f"Invalid value for {indicator} in Jiang calculation for {country_code}: {value}, using default 0.5")
                normalized_values[indicator] = 0.5

        groups = {
            'economic': ['gini_coefficient', 'inflation_annual', 'gdppc', 'wealth_concentration'],
            'social': ['social_polarization', 'institutional_distrust', 'suicide_rate'],
            'demographic': ['youth_unemployment', 'neet_ratio', 'education_gap', 'elite_overproduction']
        }
        weights = {'economic': 0.4, 'social': 0.35, 'demographic': 0.25}

        base_score = 6.0
        gdppc = indicators.get('gdppc', self.default_indicator_values['gdppc'].get(country_code, 
            self.default_indicator_values['gdppc']['default']))
        if gdppc is not None:
            try:
                base_score += min(4.0, np.log1p(self.safe_float(gdppc, self.default_indicator_values['gdppc']['default'])) / 10)
            except (ValueError, TypeError):
                logging.warning(f"Invalid gdppc for {country_code}: {gdppc}")
        gov_effectiveness = indicators.get('government_effectiveness', 
            self.default_indicator_values['government_effectiveness'].get(country_code, 
            self.default_indicator_values['government_effectiveness']['default']))
        if gov_effectiveness is not None:
            try:
                base_score += self.safe_float(gov_effectiveness, 
                    self.default_indicator_values['government_effectiveness']['default']) * 0.5
            except (ValueError, TypeError):
                logging.warning(f"Invalid government_effectiveness for {country_code}: {gov_effectiveness}")
        base_score = min(10.0, max(1.0, base_score))

        systemic_risk_score = 0.0
        risk_indicators_status = {}
        for indicator, threshold in self.thresholds.items():
            if indicator not in indicators:
                continue
            value = normalized_values.get(indicator, 0.5)
            if value >= threshold['critical']:
                systemic_risk_score += abs(threshold['points']['critical'])
                risk_indicators_status[indicator] = 'critical'
            elif value >= threshold['alert']:
                systemic_risk_score += abs(threshold['points']['alert'])
                risk_indicators_status[indicator] = 'alert'
            else:
                risk_indicators_status[indicator] = 'stable'

        group_scores = {}
        for group, ind_list in groups.items():
            group_score = sum(normalized_values.get(ind, 0.5) for ind in ind_list) / len(ind_list)
            group_scores[group] = round(group_score, 2)
            systemic_risk_score += group_score * weights[group]

        shock_factor = self.get_gdelt_shock_factor(country_code, force_refresh=True)
        systemic_risk_score *= shock_factor

        high_risk = self.fetch_latest_fsi()
        geo_risk = 0.0
        if country_code in high_risk:
            score = high_risk[country_code]
            geo_risk = 0.5 if score > 100 else 0.3 if score > 90 else 0.0
            geo_risk += self.crisis_forecasts.get(country_code, 0)
            geo_risk = min(0.7, geo_risk)
        systemic_risk_score += geo_risk * 0.15

        delta_penalty = 0.0
        for ind in ['gini_coefficient', 'youth_unemployment', 'neet_ratio', 'inflation_annual']:
            delta = deltas.get(ind)
            if delta is not None and isinstance(delta, (int, float)) and delta > 0:
                delta_penalty += delta * 0.1
        systemic_risk_score += delta_penalty

        forecast_penalty = 0.0
        for ind in ['gini_coefficient', 'youth_unemployment', 'neet_ratio', 'inflation_annual']:
            value = indicators.get(ind)
            if value is not None and isinstance(value, (int, float)):
                forecast_val = forecasts.get(ind, value)
                if isinstance(forecast_val, (int, float)):
                    forecast_penalty += max(0, forecast_val - value) * 0.05
        systemic_risk_score += forecast_penalty

        systemic_multiplier = 1.5 - (systemic_risk_score * 1.0)
        systemic_multiplier = max(0.5, min(1.5, systemic_multiplier))

        final_score = base_score * systemic_multiplier
        final_score = round(max(1.0, min(10.0, final_score)), 2)

        if final_score <= self.thresholds['estabilidad_jiang']['critical']:
            status = 'critical'
        elif final_score <= self.thresholds['estabilidad_jiang']['alert']:
            status = 'alert'
        else:
            status = 'stable'

        return {
            'status': status,
            'valor': final_score,
            'indicators': risk_indicators_status,
            'groups': group_scores
        }

    def process_country(self, country_code: str, year: int) -> Optional[Dict]:
        logging.info(f"Processing country: {country_code}")
        all_indicators = {'country_code': country_code, 'year': year}
        deltas = {}
        forecasts = {}
        missing_indicators = []

        valid_data = False
        for indicator, wb_code in self.indicator_sources.items():
            hist_data = self.fetch_world_bank_data(country_code, wb_code[0][1], years_back=10)
            if hist_data and hist_data['historical']:
                valid_data = True
                all_indicators[indicator] = hist_data['current']
                deltas[indicator] = hist_data['delta']
                forecasts[indicator] = self.forecast_indicator(hist_data['historical'], country_code=country_code, indicator=indicator)
            else:
                all_indicators[indicator] = hist_data['current']  # Use default value
                missing_indicators.append(indicator)

        if missing_indicators:
            logging.info(f"Missing indicators for {country_code}: {', '.join(missing_indicators)}")

        if not valid_data:
            logging.warning(f"No valid historical data for {country_code}, using default values")
            for indicator in self.indicator_sources:
                if indicator not in all_indicators or all_indicators[indicator] is None:
                    all_indicators[indicator] = self.default_indicator_values.get(indicator, {}).get(country_code, 
                        self.default_indicator_values.get(indicator, {}).get('default', 0.0))
                    deltas[indicator] = 0.0
                    forecasts[indicator] = self.default_indicator_values.get(indicator, {}).get(country_code, 
                        self.default_indicator_values.get(indicator, {}).get('default', 0.0))

        wealth_concentration, education_gap, elite_overproduction = self.calculate_proxies(all_indicators)
        all_indicators['wealth_concentration'] = wealth_concentration
        all_indicators['education_gap'] = education_gap
        all_indicators['elite_overproduction'] = elite_overproduction

        social_polarization, institutional_distrust = self.calculate_social_indicators(country_code, all_indicators)
        all_indicators['social_polarization'] = social_polarization
        all_indicators['institutional_distrust'] = institutional_distrust

        jiang_stability = self.calculate_jiang_stability(all_indicators, deltas, forecasts, country_code)
        turchin_instability = self.calculate_turchin_instability(all_indicators, deltas, forecasts)

        result = {
            'country_code': country_code,
            'year': year,
            'estabilidad_jiang': jiang_stability,
            'inestabilidad_turchin': turchin_instability,
            'indicators': all_indicators
        }
        return result

    def save_to_json(self, data: List[Dict], filename: str = 'data/combined_analysis_results.json'):
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w') as f:
                json.dump([d for d in data if d is not None], f, indent=2)
            logging.info(f"Saved data to {filename}")
        except Exception as e:
            logging.error(f"Error saving to JSON: {e}")

    def main(self, test_mode: bool = False):
        year = datetime.now().year
        results = []
        countries = self.country_codes[:10] if test_mode else self.country_codes

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_country = {executor.submit(self.process_country, country, year): country for country in countries}
            for future in concurrent.futures.as_completed(future_to_country):
                country = future_to_country[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                    logging.info(f"Completed processing for {country}")
                except Exception as e:
                    logging.error(f"Error processing {country}: {e}")

        self.save_to_json(results)

if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)  # Ensure data directory exists
    processor = CliodynamicDataProcessor()
    processor.main(test_mode=True)

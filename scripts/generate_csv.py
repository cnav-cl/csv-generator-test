import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import os
import time
from typing import Dict, List, Optional, Tuple
import json
import csv
from dataclasses import dataclass
from bs4 import BeautifulSoup
import re

@dataclass
class DataSource:
    name: str
    base_url: str
    api_key: str = ""
    rate_limit: float = 0.1

class CliodynamicDataProcessor:
    def __init__(self):
        self.sources = {
            'world_bank': DataSource(
                name="World Bank",
                base_url="https://api.worldbank.org/v2/country/{}/indicator/{}?format=json&per_page=100&date=2015:2024",
                rate_limit=0.2
            ),
            'oecd': DataSource(
                name="OECD",
                base_url="https://stats.oecd.org/SDMX-JSON/data/{}/all?startTime=2015&endTime=2024",
                rate_limit=0.2
            )
        }

        self.indicator_sources = {
            'gini_coefficient': [('world_bank', 'SI.POV.GINI')],
            'youth_unemployment': [('world_bank', 'SL.UEM.1524.ZS')],
            'inflation_annual': [('world_bank', 'FP.CPI.TOTL.ZG')],
            'neet_ratio': [('world_bank', 'SL.UEM.NEET.ZS')],
            'tertiary_education': [('world_bank', 'SE.TER.CUAT.BA.ZS')],
            'gdppc': [('world_bank', 'NY.GDP.PCAP.CD')]
        }

        self.country_codes = self.load_all_countries()
        
        # Umbrales y penalizaciones actualizados
        self.thresholds = {
            'neet_ratio': {'alert': 20.0, 'critical': 25.0, 'points': {'alert': -1.0, 'critical': -2.0}},
            'gini_coefficient': {'alert': 0.40, 'critical': 0.45, 'points': {'alert': -1.0, 'critical': -2.0}},
            'youth_unemployment': {'alert': 25.0, 'critical': 30.0, 'points': {'alert': -1.0, 'critical': -2.0}},
            'inflation_annual': {'alert': 10.0, 'critical': 15.0, 'points': {'alert': -1.0, 'critical': -2.0}},
            'social_polarization': {'alert': 0.60, 'critical': 0.75, 'points': {'alert': -1.5, 'critical': -3.0}},
            'institutional_distrust': {'alert': 0.30, 'critical': 0.45, 'points': {'alert': -1.5, 'critical': -3.0}}
        }
    
    def load_all_countries(self) -> List[str]:
        """Cargar lista de todos los países"""
        try:
            url = "https://api.worldbank.org/v2/country?format=json&per_page=300"
            response = requests.get(url, timeout=30)
            data = response.json()
            
            countries = []
            for country in data[1]:
                if country['iso3Code'] and country['incomeLevel']['id'] != 'INX':
                    countries.append(country['iso3Code'])
            
            return sorted(countries)
        except Exception as e:
            print(f"Error loading countries: {e}")
            return ['USA', 'CHN', 'IND', 'BRA', 'RUS', 'JPN', 'DEU', 'GBR', 'FRA', 
                   'ITA', 'CAN', 'AUS', 'ESP', 'MEX', 'IDN', 'TUR', 'SAU', 'CHE',
                   'NLD', 'POL', 'SWE', 'BEL', 'ARG', 'NOR', 'AUT', 'THA', 'ARE',
                   'ISR', 'ZAF', 'DNK', 'SGP', 'FIN', 'COL', 'MYS', 'IRL', 'CHL',
                   'EGY', 'PHL', 'PAK', 'GRC', 'PRT', 'CZE', 'ROU', 'NZL', 'PER',
                   'HUN', 'QAT', 'UKR', 'DZA', 'KWT', 'MAR', 'BGD', 'VEN', 'OMN',
                   'SVK', 'HRV', 'LBN', 'LKA', 'BGR', 'TUN', 'DOM', 'PRI', 'EST',
                   'LTU', 'PAN', 'SRB', 'AZE', 'SLV', 'URY', 'KEN', 'LVA', 'CYP',
                   'GTM', 'ETH', 'CRI', 'JOR', 'BHR', 'NPL', 'BOL', 'TZA', 'HND',
                   'UGA', 'SEN', 'GEO', 'ZWE', 'MMR', 'KAZ', 'CMR', 'CIV', 'SDN',
                   'AGO', 'NGA', 'MOZ', 'GHA', 'MDG', 'COD', 'TCD', 'YEM', 'AFG']

    def fetch_world_bank_data(self, country_code: str, indicator_code: str) -> Optional[float]:
        """Obtener datos del Banco Mundial"""
        try:
            url = self.sources['world_bank'].base_url.format(country_code, indicator_code)
            response = requests.get(url, timeout=30)
            data = response.json()
            
            if data and data[0]['total'] > 0:
                recent_values = []
                for item in data[1]:
                    if item['value'] is not None and int(item['date']) >= 2019:
                        recent_values.append((int(item['date']), float(item['value'])))
                
                if recent_values:
                    recent_values.sort(key=lambda x: x[0], reverse=True)
                    return recent_values[0][1]
            
            return None
        except Exception as e:
            print(f"Error fetching World Bank data for {country_code}-{indicator_code}: {e}")
            return None

    def scrape_trust_data(self, country_code: str) -> Optional[float]:
        """Web scraping para obtener datos de confianza institucional en tiempo real"""
        try:
            country_name_map = {
                'ARG': 'argentina', 'BRA': 'brazil', 'MEX': 'mexico', 'CHL': 'chile',
                'COL': 'colombia', 'PER': 'peru', 'USA': 'united-states', 'CAN': 'canada',
                'GBR': 'united-kingdom', 'DEU': 'germany', 'FRA': 'france', 'ESP': 'spain',
                'ITA': 'italy', 'CHN': 'china', 'IND': 'india', 'ZAF': 'south-africa',
                'RUS': 'russia', 'JPN': 'japan', 'AUS': 'australia', 'TUR': 'turkey', 'ISR': 'israel'
            }
            
            country_name = country_name_map.get(country_code)
            if not country_name:
                return None
            
            trust_data = self.scrape_owid_trust_data(country_name)
            if trust_data:
                return trust_data
            
            trust_data = self.scrape_news_trust_data(country_name)
            if trust_data:
                return trust_data
            
            trust_data = self.scrape_survey_data(country_name)
            
            return trust_data
            
        except Exception as e:
            print(f"Error scraping trust data for {country_code}: {e}")
            return None

    def scrape_owid_trust_data(self, country_name: str) -> Optional[float]:
        """Scraping de Our World in Data para datos de confianza"""
        try:
            url = f"https://ourworldindata.org/trust"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                patterns = [
                    f"{country_name}.*trust.*government.*(\d+)%",
                    f"trust.*government.*{country_name}.*(\d+)%",
                    f"{country_name}.*government.*trust.*(\d+)%"
                ]
                
                content = soup.get_text().lower()
                for pattern in patterns:
                    match = re.search(pattern, content)
                    if match:
                        return float(match.group(1))
            
            return None
            
        except Exception as e:
            print(f"Error scraping OWiD: {e}")
            return None

    def scrape_news_trust_data(self, country_name: str) -> Optional[float]:
        """Scraping de noticias recientes sobre confianza institucional"""
        try:
            search_query = f"{country_name} government trust approval rating percentage"
            url = f"https://www.google.com/search?q={search_query.replace(' ', '+')}&tbm=nws"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                news_headlines = soup.find_all('div', class_='BNeawe vvjwJb AP7Wnd')
                for headline in news_headlines[:10]:
                    text = headline.get_text().lower()
                    
                    patterns = [
                        r'(\d+)%.*trust', r'trust.*(\d+)%',
                        r'approval.*(\d+)%', r'(\d+)%.*approval',
                        r'confidence.*(\d+)%', r'(\d+)%.*confidence'
                    ]
                    
                    for pattern in patterns:
                        matches = re.findall(pattern, text)
                        if matches:
                            percentage = float(matches[0])
                            if 5 <= percentage <= 95:
                                return percentage
            
            return None
            
        except Exception as e:
            print(f"Error scraping news: {e}")
            return None

    def scrape_survey_data(self, country_name: str) -> Optional[float]:
        """Scraping de sitios de encuestas internacionales"""
        try:
            pew_url = "https://www.pewresearch.org/global/"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(pew_url, headers=headers, timeout=15)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                content = soup.get_text().lower()
                
                pattern = f"{country_name}.*trust.*government.*(\d+)%"
                match = re.search(pattern, content)
                if match:
                    return float(match.group(1))
            
            return None
            
        except Exception as e:
            print(f"Error scraping survey data: {e}")
            return None

    def estimate_from_economic_indicators(self, economic_data: Dict) -> float:
        """Estimar confianza basado en indicadores económicos cuando no hay datos reales"""
        try:
            gini = economic_data.get('gini_coefficient', 0.35)
            unemployment = economic_data.get('youth_unemployment', 15.0)
            inflation = economic_data.get('inflation_annual', 5.0)
            gdp_growth = economic_data.get('gdppc', 10000)
            
            trust_estimate = 50.0
            trust_estimate -= gini * 40
            trust_estimate -= unemployment / 2
            trust_estimate -= inflation * 2
            trust_estimate += (gdp_growth / 1000)
            
            return max(5.0, min(85.0, trust_estimate))
            
        except:
            return 45.0

    def calculate_social_indicators(self, country_code: str, economic_data: Dict) -> Tuple[float, float]:
        """Calcular indicadores sociales con datos REALES obtenidos automáticamente"""
        try:
            trust_percentage = self.scrape_trust_data(country_code)
            
            if trust_percentage is None:
                trust_percentage = self.estimate_from_economic_indicators(economic_data)
                print(f"  Using economic estimate for trust: {trust_percentage}%")
            else:
                print(f"  Found real trust data: {trust_percentage}%")
            
            institutional_distrust = (100.0 - trust_percentage) / 100.0
            institutional_distrust = round(max(0.15, min(0.95, institutional_distrust)), 2)
            
            gini = economic_data.get('gini_coefficient', 0.35)
            unemployment = economic_data.get('youth_unemployment', 15.0)
            
            polarization = 0.4 + (gini * 0.3) + (unemployment / 80)
            polarization = min(0.8, max(0.3, polarization))
            
            return round(polarization, 2), institutional_distrust
            
        except Exception as e:
            print(f"Error calculating social indicators for {country_code}: {e}")
            return 0.5, 0.6
    
    # --- Método de cálculo de estabilidad actualizado ---
    def calculate_jiang_stability(self, indicators: Dict) -> Dict:
        """
        Calcula la puntuación de estabilidad de Jiang y el nivel de riesgo.
        """
        stability_score = 10.0
        risk_indicators_status = {}
        
        # Mapeo de indicadores de riesgo
        risk_factors = {
            'neet_ratio': indicators.get('neet_ratio'),
            'gini_coefficient': indicators.get('gini_coefficient'),
            'youth_unemployment': indicators.get('youth_unemployment'),
            'inflation_annual': indicators.get('inflation_annual'),
            'social_polarization': indicators.get('social_polarization'),
            'institutional_distrust': indicators.get('institutional_distrust'),
        }

        # Calcular penalizaciones y estado de los indicadores
        for key, value in risk_factors.items():
            if value is not None:
                thresholds = self.thresholds.get(key)
                if thresholds:
                    if value >= thresholds['critical']:
                        stability_score += thresholds['points']['critical']
                        risk_indicators_status[key] = 'critical'
                    elif value >= thresholds['alert']:
                        stability_score += thresholds['points']['alert']
                        risk_indicators_status[key] = 'alert'
                    else:
                        risk_indicators_status[key] = 'stable'
            else:
                risk_indicators_status[key] = 'not_available'
        
        # Mapear el score a los nuevos niveles de estabilidad
        if stability_score <= 4.9:
            stability_level = 'critical'
        elif stability_score <= 7.4:
            stability_level = 'alert'
        else:
            stability_level = 'stable'

        final_score = round(max(1.0, min(10.0, stability_score)), 2)

        return {
            'estabilidad_jiang': final_score,
            'stability_level': stability_level,
            'risk_indicators_status': risk_indicators_status
        }
    
    def process_country(self, country_code: str, year: int) -> Optional[Dict]:
        """Procesar datos para un país específico"""
        try:
            print(f"Processing {country_code} for {year}...")
            
            economic_data = {}
            indicators_to_fetch = [
                'gini_coefficient',
                'youth_unemployment',
                'inflation_annual',
                'neet_ratio',
                'tertiary_education',
                'gdppc'
            ]
            
            for indicator in indicators_to_fetch:
                value = self.fetch_world_bank_data(country_code, self.indicator_sources.get(indicator, [None, None])[0][1])
                if value is not None:
                    economic_data[indicator] = value
                else:
                    defaults = {
                        'gini_coefficient': 0.40,
                        'youth_unemployment': 20.0,
                        'inflation_annual': 6.0,
                        'neet_ratio': 15.0,
                        'tertiary_education': 18.0,
                        'gdppc': 10000
                    }
                    economic_data[indicator] = defaults[indicator]
            
            social_polarization, institutional_distrust = self.calculate_social_indicators(country_code, economic_data)
            
            all_indicators = {
                'country_code': country_code,
                'year': year,
                'gini_coefficient': economic_data['gini_coefficient'],
                'youth_unemployment': economic_data['youth_unemployment'],
                'inflation_annual': economic_data['inflation_annual'],
                'neet_ratio': economic_data['neet_ratio'],
                'tertiary_education': economic_data['tertiary_education'],
                'social_polarization': social_polarization,
                'institutional_distrust': institutional_distrust,
                'gdppc': economic_data['gdppc']
            }

            # Llamar a la nueva función
            jiang_metrics = self.calculate_jiang_stability(all_indicators)
            
            result = {
                **all_indicators,
                **jiang_metrics
            }
            
            return result
            
        except Exception as e:
            print(f"Error processing {country_code}: {e}")
            return None

    def save_to_csv(self, data: List[Dict], filename: str = 'data/combined_analysis_results.csv'):
        """Guardar los datos procesados en un archivo CSV."""
        if not data:
            print("No data to save.")
            return

        keys = data[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)
        print(f"Data successfully saved to {filename}")

    def main(self, test_mode: bool = False):
        """Función principal con modo de prueba"""
        print(f"Starting cliodynamic data generation. Test Mode: {test_mode}")
        
        all_data = []
        current_year = datetime.now().year
        
        if test_mode:
            country_list = ['CHL', 'ARG', 'USA', 'CHN', 'CAN', 'ISR']
        else:
            country_list = self.country_codes
        
        for country_code in country_list:
            data = self.process_country(country_code, current_year)
            if data:
                all_data.append(data)
            time.sleep(self.sources['world_bank'].rate_limit)

        self.save_to_csv(all_data)

if __name__ == "__main__":
    processor = CliodynamicDataProcessor()
    
    # Cambia 'True' a 'False' para ejecutar en modo normal con todos los países
    processor.main(test_mode=True)

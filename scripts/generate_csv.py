import pandas as pd
import numpy as np
from datetime import datetime
import requests
import os
import time
from typing import Dict, List, Optional, Tuple
import json
import csv
from dataclasses import dataclass
import re
import random

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
        
        self.country_codes = self.load_all_countries()
        
        self.thresholds = {
            'neet_ratio': {'alert': 20.0, 'critical': 25.0, 'points': {'alert': -1.0, 'critical': -2.0}},
            'gini_coefficient': {'alert': 0.40, 'critical': 0.45, 'points': {'alert': -1.5, 'critical': -3.0}},
            'youth_unemployment': {'alert': 25.0, 'critical': 30.0, 'points': {'alert': -1.0, 'critical': -2.0}},
            'inflation_annual': {'alert': 10.0, 'critical': 15.0, 'points': {'alert': -1.0, 'critical': -2.0}},
            'social_polarization': {'alert': 0.60, 'critical': 0.75, 'points': {'alert': -1.5, 'critical': -3.0}},
            'institutional_distrust': {'alert': 0.30, 'critical': 0.45, 'points': {'alert': -1.5, 'critical': -3.0}},
            'suicide_rate': {'alert': 10.0, 'critical': 15.0, 'points': {'alert': -1.0, 'critical': -2.0}},
            'wealth_concentration': {'alert': 0.45, 'critical': 0.55, 'points': {'alert': -1.5, 'critical': -3.0}},
            'education_gap': {'alert': 0.05, 'critical': 0.1, 'points': {'alert': -1.0, 'critical': -2.5}},
            'elite_overproduction': {'alert': 0.05, 'critical': 0.1, 'points': {'alert': -1.5, 'critical': -3.0}}
        }
    
    def load_all_countries(self) -> List[str]:
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

    def convert_effectiveness_to_distrust(self, effectiveness: float) -> float:
        """
        Convierte el índice de efectividad del Banco Mundial (-2.5 a 2.5)
        en un valor de desconfianza (0 a 1).
        """
        normalized_effectiveness = (effectiveness - (-2.5)) / (2.5 - (-2.5))
        distrust = 1.0 - normalized_effectiveness
        return round(max(0.1, min(0.9, distrust)), 2)
    
    def calculate_proxies(self, all_indicators: Dict) -> Tuple[float, float, float]:
        """Calcula los valores de los nuevos indicadores usando proxies normalizados."""
        wealth_concentration = all_indicators.get('gini_coefficient', 40.0) / 100
        
        tertiary_education = all_indicators.get('tertiary_education', 18.0) / 100
        youth_unemployment = all_indicators.get('youth_unemployment', 20.0) / 100
        
        education_gap = tertiary_education * youth_unemployment
        elite_overproduction = tertiary_education * youth_unemployment

        return wealth_concentration, education_gap, elite_overproduction

    def calculate_social_indicators(self, country_code: str, all_indicators: Dict) -> Tuple[float, float]:
        """Calcula la polarización social y la desconfianza institucional"""
        try:
            gov_effectiveness = all_indicators.get('government_effectiveness')
            if gov_effectiveness is not None:
                institutional_distrust = self.convert_effectiveness_to_distrust(gov_effectiveness)
                print(f"  Using Government Effectiveness index ({gov_effectiveness}) to estimate institutional distrust: {institutional_distrust}")
            else:
                institutional_distrust = 0.5
                print("  Government Effectiveness data not available, using default value for distrust.")

            gini_normalized = all_indicators.get('gini_coefficient', 40.0) / 100
            neet_ratio = all_indicators.get('neet_ratio', 15.0)

            polarization = (gini_normalized * 0.4) + (institutional_distrust * 0.4) + (neet_ratio / 100 * 0.2)
            polarization = min(0.9, max(0.3, polarization))
            
            return round(polarization, 2), institutional_distrust
            
        except Exception as e:
            print(f"Error calculating social indicators for {country_code}: {e}")
            return 0.5, 0.6
    
    def calculate_jiang_stability(self, indicators: Dict) -> Dict:
        """
        Calcula la puntuación de estabilidad de Jiang y el nivel de riesgo.
        """
        stability_score = 10.0
        risk_indicators_status = {}
        
        social_media_penalty = 0.0
        if indicators.get('institutional_distrust', 0.0) >= 0.5:
            social_media_penalty = -0.5
            print(f"  Significant social media influence detected. Applying {social_media_penalty} penalty.")
        
        stability_score += social_media_penalty

        risk_factors = {
            'neet_ratio': indicators.get('neet_ratio'),
            'gini_coefficient': indicators.get('gini_coefficient'),
            'youth_unemployment': indicators.get('youth_unemployment'),
            'inflation_annual': indicators.get('inflation_annual'),
            'social_polarization': indicators.get('social_polarization'),
            'institutional_distrust': indicators.get('institutional_distrust'),
            'suicide_rate': indicators.get('suicide_rate'),
            'wealth_concentration': indicators.get('wealth_concentration'),
            'education_gap': indicators.get('education_gap'),
            'elite_overproduction': indicators.get('elite_overproduction')
        }

        for key, value in risk_factors.items():
            if value is not None:
                thresholds = self.thresholds.get(key)
                if thresholds:
                    if key in ['gini_coefficient', 'youth_unemployment', 'tertiary_education', 'wealth_concentration', 'education_gap', 'elite_overproduction', 'social_polarization', 'institutional_distrust']:
                        normalized_value = value
                        if key in ['gini_coefficient', 'youth_unemployment']:
                            normalized_value = value / 100
                        
                        if normalized_value >= thresholds['critical']:
                            stability_score += thresholds.get('points', {}).get('critical', -2.0)
                            risk_indicators_status[key] = {'status': 'critical', 'valor': value}
                        elif normalized_value >= thresholds['alert']:
                            stability_score += thresholds.get('points', {}).get('alert', -1.0)
                            risk_indicators_status[key] = {'status': 'alert', 'valor': value}
                        else:
                            risk_indicators_status[key] = {'status': 'stable', 'valor': value}
                    else:
                        if value >= thresholds['critical']:
                            stability_score += thresholds.get('points', {}).get('critical', -2.0)
                            risk_indicators_status[key] = {'status': 'critical', 'valor': value}
                        elif value >= thresholds['alert']:
                            stability_score += thresholds.get('points', {}).get('alert', -1.0)
                            risk_indicators_status[key] = {'status': 'alert', 'valor': value}
                        else:
                            risk_indicators_status[key] = {'status': 'stable', 'valor': value}
            else:
                risk_indicators_status[key] = {'status': 'not_available', 'valor': None}
        
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
            
            all_indicators = {'country_code': country_code, 'year': year}

            indicators_to_fetch = {
                'gini_coefficient': 'SI.POV.GINI',
                'youth_unemployment': 'SL.UEM.1524.ZS',
                'inflation_annual': 'FP.CPI.TOTL.ZG',
                'neet_ratio': 'SL.UEM.NEET.ZS',
                'tertiary_education': 'SE.TER.CUAT.BA.ZS',
                'gdppc': 'NY.GDP.PCAP.CD',
                'suicide_rate': 'SH.STA.SUIC.P5',
                'government_effectiveness': 'GE.EST'
            }

            for indicator, wb_code in indicators_to_fetch.items():
                value = self.fetch_world_bank_data(country_code, wb_code)
                if value is not None:
                    all_indicators[indicator] = value
                else:
                    defaults = {
                        'gini_coefficient': 40.0, 'youth_unemployment': 20.0,
                        'inflation_annual': 6.0, 'neet_ratio': 15.0,
                        'tertiary_education': 18.0, 'gdppc': 10000,
                        'suicide_rate': 10.0, 'government_effectiveness': -0.25
                    }
                    all_indicators[indicator] = defaults[indicator]
            
            wealth_concentration, education_gap, elite_overproduction = self.calculate_proxies(all_indicators)
            all_indicators['wealth_concentration'] = wealth_concentration
            all_indicators['education_gap'] = education_gap
            all_indicators['elite_overproduction'] = elite_overproduction
            
            social_polarization, institutional_distrust = self.calculate_social_indicators(country_code, all_indicators)
            
            all_indicators['social_polarization'] = social_polarization
            all_indicators['institutional_distrust'] = institutional_distrust

            jiang_metrics = self.calculate_jiang_stability(all_indicators)
            
            result = {
                **all_indicators,
                'estabilidad_jiang': jiang_metrics['estabilidad_jiang'],
                'stability_level': jiang_metrics['stability_level'],
                'risk_indicators_status': jiang_metrics['risk_indicators_status']
            }
            
            return result
            
        except Exception as e:
            print(f"Error processing {country_code}: {e}")
            return None

    def save_to_json(self, data: List[Dict], filename: str = 'data/combined_analysis_results.json'):
        """Guardar los datos procesados en un archivo JSON, con cada objeto en una nueva línea."""
        if not data:
            print("No data to save.")
            return

        if not os.path.exists('data'):
            os.makedirs('data')
        
        with open(filename, 'w', encoding='utf-8') as output_file:
            for row in data:
                output_file.write(json.dumps(row) + '\n')
        print(f"Data successfully saved to {filename}")

    def main(self, test_mode: bool = False):
        """Función principal con modo de prueba"""
        print(f"Starting cliodynamic data generation. Test Mode: {test_mode}")
        
        all_data = []
        current_year = datetime.now().year
        
        if test_mode:
            country_list = ['CHL', 'AUT', 'AUS', 'BEL', 'USA']
        else:
            country_list = self.country_codes
        
        for country_code in country_list:
            data = self.process_country(country_code, current_year)
            if data:
                all_data.append(data)
            time.sleep(self.sources['world_bank'].rate_limit)

        self.save_to_json(all_data)

if __name__ == "__main__":
    processor = CliodynamicDataProcessor()
    
    processor.main(test_mode=True)

import pandas as pd
import numpy as np
from datetime import datetime
import requests
import os
import time
from typing import Dict, List, Optional, Tuple
import json
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
            'gini_coefficient': {'stable_min': None, 'stable_max': 39.9, 'alert_min': 40.0, 'alert_max': 49.9, 'critical_min': 50.0, 'critical_max': None},
            'youth_unemployment': {'stable_min': None, 'stable_max': 29.9, 'alert_min': 30.0, 'alert_max': 39.9, 'critical_min': 40.0, 'critical_max': None},
            'inflation_annual': {'stable_min': None, 'stable_max': 5.0, 'alert_min': 5.1, 'alert_max': 9.9, 'critical_min': 10.0, 'critical_max': None},
            'neet_ratio': {'stable_min': None, 'stable_max': 19.9, 'alert_min': 20.0, 'alert_max': 29.9, 'critical_min': 30.0, 'critical_max': None},
            'tertiary_education': {'stable_min': 20.0, 'stable_max': None, 'alert_min': 10.0, 'alert_max': 20.0, 'critical_min': None, 'critical_max': 10.0},
            'gdppc': {'stable_min': 15000.0, 'stable_max': None, 'alert_min': 5000.0, 'alert_max': 14999.0, 'critical_min': None, 'critical_max': 5000.0},
            'suicide_rate': {'stable_min': None, 'stable_max': 9.9, 'alert_min': 10.0, 'alert_max': 14.9, 'critical_min': 15.0, 'critical_max': None},
            'government_effectiveness': {'stable_min': 0.5, 'stable_max': None, 'alert_min': 0.0, 'alert_max': 0.49, 'critical_min': None, 'critical_max': 0.0},
            'wealth_concentration': {'stable_min': None, 'stable_max': 0.5, 'alert_min': 0.51, 'alert_max': 0.7, 'critical_min': 0.7, 'critical_max': None},
            'education_gap': {'stable_min': None, 'stable_max': 0.05, 'alert_min': 0.051, 'alert_max': 0.1, 'critical_min': 0.1, 'critical_max': None},
            'elite_overproduction': {'stable_min': None, 'stable_max': 0.05, 'alert_min': 0.051, 'alert_max': 0.1, 'critical_min': 0.1, 'critical_max': None},
            'social_polarization': {'stable_min': None, 'stable_max': 0.4, 'alert_min': 0.41, 'alert_max': 0.6, 'critical_min': 0.6, 'critical_max': None},
            'institutional_distrust': {'stable_min': None, 'stable_max': 0.35, 'alert_min': 0.36, 'alert_max': 0.5, 'critical_min': 0.5, 'critical_max': None},
            'estabilidad_jiang': {'stable_min': 7.0, 'stable_max': None, 'alert_min': 5.0, 'alert_max': 6.9, 'critical_min': None, 'critical_max': 5.0}
        }

        self.indicator_names = {
            'estabilidad_jiang': "Nivel de Estabilidad Jiang",
            'gini_coefficient': "Coeficiente de Gini",
            'institutional_distrust': "Desconfianza Institucional",
            'youth_unemployment': "Desempleo Juvenil",
            'inflation_annual': "Inflación Anual",
            'neet_ratio': "Tasa NEET (No estudia, No trabaja)",
            'tertiary_education': "Educación Terciaria",
            'gdppc': "PIB per cápita",
            'suicide_rate': "Tasa de Suicidio",
            'government_effectiveness': "Efectividad Gubernamental",
            'wealth_concentration': "Concentración de Riqueza",
            'education_gap': "Brecha Educativa",
            'elite_overproduction': "Sobreproducción de Élite",
            'social_polarization': "Polarización Social"
        }
        
        self.indicator_descriptions = {
            'estabilidad_jiang': "Una métrica agregada que evalúa la estabilidad social y económica.",
            'gini_coefficient': "Mide la desigualdad en la distribución de ingresos.",
            'institutional_distrust': "Evalúa el nivel de desconfianza de la población en las instituciones gubernamentales.",
            'youth_unemployment': "Tasa de desempleo en la población entre 15 y 24 años.",
            'inflation_annual': "Variación porcentual de los precios en un año.",
            'neet_ratio': "Porcentaje de jóvenes que no trabajan ni estudian ni están en formación.",
            'tertiary_education': "Porcentaje de la población con educación postsecundaria.",
            'gdppc': "Producto Interno Bruto por persona.",
            'suicide_rate': "Número de suicidios por cada 100,000 habitantes.",
            'government_effectiveness': "Mide la calidad de la administración pública y el servicio civil.",
            'wealth_concentration': "Evalúa el porcentaje de la riqueza total del país en manos del 1% más rico.",
            'education_gap': "La diferencia en años de educación entre los niveles socioeconómicos.",
            'elite_overproduction': "Mide la proporción de la población educada en exceso de los empleos disponibles para ellos.",
            'social_polarization': "Evalúa la división de la sociedad en grupos con opiniones extremas y opuestas."
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
                    print(f"  -> Data fetched for {indicator_code}: {recent_values[0][1]}")
                    return recent_values[0][1]
            
            print(f"  -> No data found for {indicator_code}")
            return None
        except Exception as e:
            print(f"  -> Error fetching World Bank data for {country_code}-{indicator_code}: {e}")
            return None

    def convert_effectiveness_to_distrust(self, effectiveness: float) -> float:
        normalized_effectiveness = (effectiveness - (-2.5)) / (2.5 - (-2.5))
        distrust = 1.0 - normalized_effectiveness
        return round(max(0.1, min(0.9, distrust)), 2)
    
    def calculate_proxies(self, all_indicators: Dict) -> Tuple[float, float, float]:
        wealth_concentration = all_indicators.get('gini_coefficient', 40.0) / 100
        
        tertiary_education = all_indicators.get('tertiary_education', 18.0) / 100
        youth_unemployment = all_indicators.get('youth_unemployment', 20.0) / 100
        
        education_gap = tertiary_education * youth_unemployment
        elite_overproduction = tertiary_education * youth_unemployment

        return wealth_concentration, education_gap, elite_overproduction

    def calculate_social_indicators(self, country_code: str, all_indicators: Dict) -> Tuple[float, float]:
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
            print(f"  -> Error calculating social indicators for {country_code}: {e}")
            return 0.5, 0.6
    
    def calculate_status(self, indicator_key: str, value: float) -> str:
        """Determina el estado de un indicador basado en sus umbrales."""
        thresholds = self.thresholds.get(indicator_key)
        if not thresholds or value is None:
            return "not_available"

        stable_min = thresholds.get('stable_min')
        stable_max = thresholds.get('stable_max')
        alert_min = thresholds.get('alert_min')
        alert_max = thresholds.get('alert_max')
        critical_min = thresholds.get('critical_min')
        critical_max = thresholds.get('critical_max')

        if (stable_min is None or value >= stable_min) and (stable_max is None or value <= stable_max):
            return "stable"
        if (alert_min is None or value >= alert_min) and (alert_max is None or value <= alert_max):
            return "alert"
        if (critical_min is None or value >= critical_min) and (critical_max is None or value <= critical_max):
            return "critical"
        
        return "not_available"

    def get_indicators_definitions(self) -> Dict:
        """Genera la estructura de definiciones de indicadores."""
        definitions = {}
        for key in self.thresholds.keys():
            if key == 'estabilidad_jiang':
                definitions[key] = {
                    "name": self.indicator_names.get(key),
                    "description": self.indicator_descriptions.get(key),
                    "thresholds": {
                        "stable": {"min": self.thresholds[key]['stable_min'], "max": self.thresholds[key]['stable_max']},
                        "alert": {"min": self.thresholds[key]['alert_min'], "max": self.thresholds[key]['alert_max']},
                        "critical": {"min": self.thresholds[key]['critical_min'], "max": self.thresholds[key]['critical_max']}
                    }
                }
            else:
                definitions[key] = {
                    "name": self.indicator_names.get(key),
                    "description": self.indicator_descriptions.get(key),
                    "thresholds": {
                        "stable": {"min": self.thresholds[key]['stable_min'], "max": self.thresholds[key]['stable_max']},
                        "alert": {"min": self.thresholds[key]['alert_min'], "max": self.thresholds[key]['alert_max']},
                        "critical": {"min": self.thresholds[key]['critical_min'], "max": self.thresholds[key]['critical_max']}
                    }
                }
        return definitions

    def process_country(self, country_code: str, year: int) -> Dict:
        """Procesar datos para un país específico y devolver la estructura de datos para 'country_data'."""
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

        try:
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
            
            # Formatear el resultado para la nueva estructura
            country_data = {
                "country_code": country_code,
                "year": year,
                "estabilidad_jiang": {
                    "value": jiang_metrics['estabilidad_jiang'],
                    "status": jiang_metrics['stability_level']
                },
                "indicators": {}
            }
            
            for key, value in all_indicators.items():
                if key not in ['country_code', 'year', 'estabilidad_jiang']:
                    country_data['indicators'][key] = {
                        'value': value,
                        'status': self.calculate_status(key, value)
                    }

            print(f"  -> Finished processing {country_code}. Result: {country_data.get('estabilidad_jiang')}, {country_data.get('estabilidad_jiang').get('status')}")
            return country_data
        except Exception as e:
            print(f"  -> Error processing {country_code}: {e}. Returning partial data.")
            return {
                "country_code": country_code,
                "year": year,
                "estabilidad_jiang": {"value": None, "status": "not_available"},
                "indicators": {}
            }

    def calculate_jiang_stability(self, indicators: Dict) -> Dict:
        # Esta función mantiene su lógica interna, pero el resultado se formatea en `process_country`
        stability_score = 10.0
        
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

        # Aplicar penalizaciones
        for key, value in risk_factors.items():
            if value is not None:
                status = self.calculate_status(key, value)
                if status == 'alert':
                    stability_score += -1.5 # Ejemplo de puntos de penalización
                elif status == 'critical':
                    stability_score += -3.0 # Ejemplo de puntos de penalización
        
        final_score = round(max(1.0, min(10.0, stability_score)), 2)
        stability_level = self.calculate_status('estabilidad_jiang', final_score)

        return {
            'estabilidad_jiang': final_score,
            'stability_level': stability_level
        }

    def save_to_json(self, data: Dict, filename: str = 'data/combined_analysis_results.json'):
        """Guardar los datos procesados en un archivo JSON en un solo bloque."""
        if not data:
            print("No data to save.")
            return

        if not os.path.exists('data'):
            os.makedirs('data')
        
        with open(filename, 'w', encoding='utf-8') as output_file:
            json.dump(data, output_file, indent=2, ensure_ascii=False)
        print(f"Data successfully saved to {filename}")

    def main(self, test_mode: bool = False):
        """Función principal con modo de prueba"""
        print(f"Starting cliodynamic data generation. Test Mode: {test_mode}")
        
        indicators_definitions = self.get_indicators_definitions()
        country_data_all = {}
        current_year = datetime.now().year
        
        if test_mode:
            country_list = ['CHL', 'AUT', 'AUS', 'BEL', 'USA']
        else:
            country_list = self.country_codes
        
        for country_code in country_list:
            data = self.process_country(country_code, current_year)
            country_data_all[country_code] = data
            time.sleep(self.sources['world_bank'].rate_limit)

        final_json = {
            "indicators_definitions": indicators_definitions,
            "country_data": country_data_all
        }
        
        print("\nFinal data to be saved:")
        print(json.dumps(final_json, indent=2, ensure_ascii=False))
        
        self.save_to_json(final_json)

if __name__ == "__main__":
    processor = CliodynamicDataProcessor()
    
    processor.main(test_mode=True)

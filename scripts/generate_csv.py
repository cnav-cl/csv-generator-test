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
            'gdelt': DataSource(
                name="GDELT Project",
                # La consulta ahora incluye todos los eventos de conflicto (códigos 1 a 4).
                base_url="https://api.gdeltproject.org/api/v2/query?query=sourcecountry:{country_code}%20eventcode:1*|2*|3*|4*&mode=TimelineVol&format=json&startdatetime={start_date}&enddatetime={end_date}",
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
            'elite_overproduction': {'alert': 0.05, 'critical': 0.1, 'points': {'alert': -1.5, 'critical': -3.0}},
            'estabilidad_jiang': {'alert': 5.5, 'critical': 4.0, 'points': {'alert': 0.0, 'critical': 0.0}},
            'inestabilidad_turchin': {'alert': 0.4, 'critical': 0.6, 'points': {'alert': 0.0, 'critical': 0.0}}
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
    
    def get_gdelt_shock_factor(self, country_code: str) -> float:
        """
        Calcula un factor de choque basado en el número de eventos de conflicto recientes en GDELT.
        """
        end_date = datetime.now().strftime('%Y%m%d%H%M%S')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d%H%M%S')

        try:
            url = self.sources['gdelt'].base_url.format(
                country_code=country_code,
                start_date=start_date,
                end_date=end_date
            )
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            event_counts = response.json().get('timeline', [{}])[0].get('data', [])
            total_events = sum(item.get('value', 0) for item in event_counts)
            
            if total_events > 50:
                print(f"Alerta: {country_code} con alta actividad de conflicto ({total_events} eventos). Factor de Choque = 2.5.")
                return 2.5
            elif total_events > 10:
                print(f"Advertencia: {country_code} con actividad de conflicto moderada ({total_events} eventos). Factor de Choque = 1.8.")
                return 1.8
            else:
                return 1.0
                
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener datos de GDELT para {country_code}: {e}")
            return 1.0 # Devuelve 1.0 en caso de error para no afectar el cálculo.

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

    def calculate_turchin_instability(self, indicators: Dict) -> Dict:
        """Calcular índice de inestabilidad de Turchin basado en los trabajos originales"""
        try:
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
                    if indicator == 'elite_overproduction':
                        norm_value = min(1.0, value / 0.15)
                    elif indicator == 'wealth_concentration':
                        norm_value = min(1.0, value / 0.55)
                    elif indicator == 'institutional_distrust':
                        norm_value = value
                    elif indicator == 'social_polarization':
                        norm_value = value
                    elif indicator == 'youth_unemployment':
                        norm_value = min(1.0, value / 40.0)
                    elif indicator == 'neet_ratio':
                        norm_value = min(1.0, value / 30.0)
                    
                    instability_score += weight * norm_value
            
            final_score = round(min(1.0, max(0.0, instability_score)), 2)
            
            # Determinar el estado basado en el nuevo umbral
            if final_score >= self.thresholds['inestabilidad_turchin']['critical']:
                status = 'critical'
            elif final_score >= self.thresholds['inestabilidad_turchin']['alert']:
                status = 'alert'
            else:
                status = 'stable'
            
            return {'status': status, 'valor': final_score}
            
        except Exception as e:
            print(f"Error calculating Turchin instability: {e}")
            return {'status': 'not_available', 'valor': None}

    def calculate_jiang_stability(self, indicators: Dict) -> Dict:
        """
        Calcular el Índice de Estabilidad de Jiang utilizando un modelo ponderado
        que refleja las interacciones sistémicas descritas en la teoría cliodinámica.
        """
        try:
            # 1. Normalizar todos los valores entre 0-1 para análisis comparativo
            normalized_values = {}
            
            # Normalización específica para cada indicador
            normalization_rules = {
                'gini_coefficient': lambda x: x / 100,
                'youth_unemployment': lambda x: x / 100,
                'inflation_annual': lambda x: min(1.0, x / 50),
                'neet_ratio': lambda x: x / 100,
                'tertiary_education': lambda x: x / 100,
                'suicide_rate': lambda x: min(1.0, x / 30),
                'government_effectiveness': lambda x: (x - (-2.5)) / (2.5 - (-2.5)),
                'social_polarization': lambda x: x,
                'institutional_distrust': lambda x: x,
                'wealth_concentration': lambda x: x,
                'education_gap': lambda x: min(1.0, x / 0.2),
                'elite_overproduction': lambda x: min(1.0, x / 0.2),
                'gdppc': lambda x: min(1.0, x / 80000)
            }
            
            for indicator, value in indicators.items():
                if indicator in normalization_rules and value is not None:
                    normalized_values[indicator] = normalization_rules[indicator](value)
                elif value is not None:
                    normalized_values[indicator] = min(1.0, max(0.0, value / 100 if isinstance(value, (int, float)) else 0.5))

            # 2. Definir Grupos Sistémicos y sus Ponderaciones
            systemic_weights = {
                'cohesion_group': 0.40,
                'strain_group': 0.35,
                'elite_group': 0.25
            }

            indicator_groups = {
                'cohesion_group': [
                    'institutional_distrust',
                    'social_polarization',
                    'government_effectiveness'
                ],
                'strain_group': [
                    'youth_unemployment',
                    'neet_ratio',
                    'suicide_rate',
                    'inflation_annual'
                ],
                'elite_group': [
                    'elite_overproduction',
                    'wealth_concentration',
                    'gini_coefficient',
                    'education_gap'
                ]
            }

            # 3. Calcular la Puntuación Base (Factores de Fortaleza Estructural)
            base_score = 6.0
            
            # Factores positivos (aumentan la base)
            gdppc = indicators.get('gdppc', 0)
            gov_effectiveness = indicators.get('government_effectiveness', -2.5)
            tertiary_edu = indicators.get('tertiary_education', 0)
            
            if gdppc > 20000:
                base_score += 1.5
            elif gdppc > 10000:
                base_score += 0.5
            elif gdppc < 3000:
                base_score -= 1.0
                
            if gov_effectiveness > 0.5:
                base_score += 1.0
            elif gov_effectiveness < -1.0:
                base_score -= 1.0
                
            if tertiary_edu > 30:
                base_score += 0.5
                
            base_score = max(3.0, min(8.0, base_score))

            # 4. Calcular el Multiplicador de Riesgo Sistémico
            systemic_risk_score = 0.0
            risk_indicators_status = {}

            for group_name, weight in systemic_weights.items():
                group_indicators = indicator_groups[group_name]
                group_risk = 0.0
                count = 0

                for indicator in group_indicators:
                    norm_value = normalized_values.get(indicator)
                    if norm_value is not None:
                        if indicator in ['government_effectiveness']:
                            group_risk += (1 - norm_value)
                        else:
                            group_risk += norm_value
                        count += 1
                        
                        # Determinar estado individual del indicador
                        raw_value = indicators.get(indicator)
                        if raw_value is not None:
                            thresholds = self.thresholds.get(indicator)
                            if thresholds:
                                if indicator in ['gini_coefficient', 'wealth_concentration', 
                                               'social_polarization', 'institutional_distrust',
                                               'education_gap', 'elite_overproduction']:
                                    if raw_value >= thresholds['critical']:
                                        risk_indicators_status[indicator] = {'status': 'critical', 'valor': raw_value}
                                    elif raw_value >= thresholds['alert']:
                                        risk_indicators_status[indicator] = {'status': 'alert', 'valor': raw_value}
                                    else:
                                        risk_indicators_status[indicator] = {'status': 'stable', 'valor': raw_value}
                                else:
                                    if raw_value >= thresholds['critical']:
                                        risk_indicators_status[indicator] = {'status': 'critical', 'valor': raw_value}
                                    elif raw_value >= thresholds['alert']:
                                        risk_indicators_status[indicator] = {'status': 'alert', 'valor': raw_value}
                                    else:
                                        risk_indicators_status[indicator] = {'status': 'stable', 'valor': raw_value}
                            else:
                                risk_indicators_status[indicator] = {'status': 'not_available', 'valor': raw_value}

                if count > 0:
                    group_avg_risk = group_risk / count
                    systemic_risk_score += group_avg_risk * weight

            # Convertir el riesgo sistémico a multiplicador
            systemic_multiplier = 1.5 - (systemic_risk_score * 1.0)
            systemic_multiplier = max(0.5, min(1.5, systemic_multiplier))

            # 5. Calcular Puntuación Final
            final_score = base_score * systemic_multiplier
            final_score = round(max(1.0, min(10.0, final_score)), 2)

            # 6. Determinar el nivel de estabilidad
            if final_score <= self.thresholds['estabilidad_jiang']['critical']:
                stability_level = 'critical'
            elif final_score <= self.thresholds['estabilidad_jiang']['alert']:
                stability_level = 'alert'
            else:
                stability_level = 'stable'

            risk_indicators_status['estabilidad_jiang'] = {'status': stability_level, 'valor': final_score}
            
            # 7. Log para diagnóstico
            print(f"  Jiang Stability Calculation:")
            print(f"    Base Score: {base_score}")
            print(f"    Systemic Risk: {systemic_risk_score:.2f}")
            print(f"    Multiplier: {systemic_multiplier:.2f}")
            print(f"    Final Score: {final_score} ({stability_level})")

            return {
                'risk_indicators_status': risk_indicators_status
            }
            
        except Exception as e:
            print(f"Error in calculate_jiang_stability: {e}")
            return {
                'risk_indicators_status': {
                    'estabilidad_jiang': {'status': 'not_available', 'valor': None}
                }
            }
    
    def process_country(self, country_code: str, year: int) -> Dict:
        """Procesar datos para un país específico"""
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

            # Calcular ambos índices
            jiang_metrics = self.calculate_jiang_stability(all_indicators)
            
            # Ahora Turchin devuelve un diccionario
            turchin_instability_dict = self.calculate_turchin_instability(all_indicators)
            
            # Mover 'inestabilidad_turchin' dentro de 'risk_indicators_status'
            jiang_metrics['risk_indicators_status']['inestabilidad_turchin'] = turchin_instability_dict
            
            result = {
                'country_code': country_code,
                'year': year,
                'indicators': all_indicators,
                'risk_indicators_status': jiang_metrics['risk_indicators_status']
            }
            
            # Obtener el factor de choque dinámico
            shock_factor = self.get_gdelt_shock_factor(country_code)

            # Aplicar el factor de choque a los índices finales
            result['risk_indicators_status']['estabilidad_jiang']['valor'] /= shock_factor
            result['risk_indicators_status']['inestabilidad_turchin']['valor'] *= shock_factor
            
            # Vuelva a determinar el estado con el nuevo valor del factor de choque
            final_jiang = result['risk_indicators_status']['estabilidad_jiang']['valor']
            if final_jiang <= self.thresholds['estabilidad_jiang']['critical']:
                result['risk_indicators_status']['estabilidad_jiang']['status'] = 'critical'
            elif final_jiang <= self.thresholds['estabilidad_jiang']['alert']:
                result['risk_indicators_status']['estabilidad_jiang']['status'] = 'alert'
            else:
                result['risk_indicators_status']['estabilidad_jiang']['status'] = 'stable'

            final_turchin = result['risk_indicators_status']['inestabilidad_turchin']['valor']
            if final_turchin >= self.thresholds['inestabilidad_turchin']['critical']:
                result['risk_indicators_status']['inestabilidad_turchin']['status'] = 'critical'
            elif final_turchin >= self.thresholds['inestabilidad_turchin']['alert']:
                result['risk_indicators_status']['inestabilidad_turchin']['status'] = 'alert'
            else:
                result['risk_indicators_status']['inestabilidad_turchin']['status'] = 'stable'


            print(f"  -> Finished processing {country_code}. Result: {result.get('risk_indicators_status', {}).get('estabilidad_jiang', {}).get('valor')}, {result.get('risk_indicators_status', {}).get('estabilidad_jiang', {}).get('status')}")
            return result
        except Exception as e:
            print(f"  -> Error processing {country_code}: {e}. Returning partial data.")
            all_indicators['wealth_concentration'] = None
            all_indicators['education_gap'] = None
            all_indicators['elite_overproduction'] = None
            all_indicators['social_polarization'] = None
            all_indicators['institutional_distrust'] = None
            
            result = {
                'country_code': country_code,
                'year': year,
                'indicators': all_indicators,
                'risk_indicators_status': {}
            }
            
            return result

    def save_to_json(self, data: List[Dict], filename: str = 'data/combined_analysis_results.json'):
        """Guardar los datos procesados en un archivo JSON, con cada objeto en una nueva línea."""
        if not data:
            print("No data to save.")
            return

        if not os.path.exists('data'):
            os.makedirs('data')
        
        formatted_data = {
            "metadata": {
                "creation_date": datetime.now().isoformat(),
                "test_mode": True
            },
            "country_data": {}
        }
        for item in data:
            country_code = item.pop('country_code')
            formatted_data['country_data'][country_code] = item
        
        with open(filename, 'w', encoding='utf-8') as output_file:
            json.dump(formatted_data, output_file, indent=4, ensure_ascii=False)
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
            all_data.append(data)
            time.sleep(self.sources['world_bank'].rate_limit)
        
        self.save_to_json(all_data)

if __name__ == "__main__":
    processor = CliodynamicDataProcessor()
    
    processor.main(test_mode=False)

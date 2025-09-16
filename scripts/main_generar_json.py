import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import time
from typing import Dict, List, Optional, Tuple
import json
from dataclasses import dataclass
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CliodynamicDataProcessor:
    DATA_DIR = 'data'

    def __init__(self, cache_file: str = os.path.join(DATA_DIR, 'cache.json')):
        self.cache_file = cache_file
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
            'happiness_score': 'WHR.SCORE',
            'traditional_vs_secular': 'CULTURAL_TRADITIONAL_SECULAR',
            'survival_vs_self_expression': 'CULTURAL_SURVIVAL_SELFEXPRESSION',
            'social_cohesion_index': 'CULTURAL_SOCIAL_COHESION'
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
            'WHR.SCORE': {'default': 5.0},
            'CULTURAL_TRADITIONAL_SECULAR': {'default': 0.0},
            'CULTURAL_SURVIVAL_SELFEXPRESSION': {'default': 0.0},
            'CULTURAL_SOCIAL_COHESION': {'default': 0.5}
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
            'traditional_vs_secular': 'estatico',
            'survival_vs_self_expression': 'estatico',
            'social_cohesion_index': 'estatico'
        }
        
        self.current_year = datetime.now().year
        self._load_cultural_data()
        
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
            'NZL': [],
            'HKG': ['CHN'],
            'ISR': ['EGY', 'JOR', 'SYR', 'LBN'],
            'ARE': ['OMN', 'SAU']
        }
    
    def _load_json_data(self, file_path: str) -> Optional[Dict]:
        """Carga datos de un archivo JSON."""
        try:
            full_path = os.path.join(self.DATA_DIR, file_path)
            with open(full_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logging.info(f"Datos cargados exitosamente de {full_path}")
            return data
        except FileNotFoundError:
            logging.error(f"Error: El archivo {full_path} no fue encontrado.")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"Error al decodificar JSON de {full_path}: {e}")
            return None
        except Exception as e:
            logging.error(f"Error inesperado al leer {full_path}: {e}")
            return None

    def _load_cultural_data(self):
        """Carga datos culturales desde el archivo JSON local."""
        file_name = "data_worldsurvey_valores.json"
        data = self._load_json_data(file_name)
        if data and "countries" in data:
            self.cultural_data = data["countries"]
            logging.info("Datos culturales cargados exitosamente desde JSON local.")
        else:
            self.cultural_data = {}
            logging.warning("No se pudieron cargar datos culturales desde el archivo JSON. Se usará un diccionario vacío.")

    def calculate_turchin_instability(self, indicators: Dict[str, float], border_pressure: float) -> Dict:
        """Calcula el \u00edndice de inestabilidad de Turchin basado en los indicadores."""
        
        factors = {
            'youth_unemployment': indicators.get('youth_unemployment', 0) / 100,
            'gini_coefficient': indicators.get('gini_coefficient', 0) / 100,
            'elite_overproduction': indicators.get('elite_overproduction', 0),
            'social_polarization': indicators.get('social_polarization', 0),
            'institutional_distrust': indicators.get('institutional_distrust', 0),
            'border_pressure': border_pressure
        }
        
        # Ponderaciones
        weights = {
            'youth_unemployment': 0.25,
            'gini_coefficient': 0.25,
            'elite_overproduction': 0.2,
            'social_polarization': 0.15,
            'institutional_distrust': 0.15,
            'border_pressure': 0.1
        }
        
        instability_score = sum(factors[key] * weights.get(key, 0) for key in factors)
        
        if instability_score > 0.4:
            status = "at_risk"
        elif instability_score > 0.6:
            status = "fragile"
        else:
            status = "stable"
            
        return {
            "status": status,
            "valor": round(instability_score, 2),
            "comment": "Calculado basado en indicadores internos y presi\u00f3n fronteriza."
        }
    
    def calculate_border_pressure(self, country_code: str, all_country_results: Dict[str, Dict]) -> float:
        """Calcula la presi\u00f3n fronteriza de un pa\u00eds."""
        
        borders = self.border_mapping.get(country_code, [])
        total_pressure = 0.0
        if not borders:
            return 0.0

        for neighbor_code in borders:
            neighbor_data = all_country_results.get(neighbor_code)
            if neighbor_data:
                # La presi\u00f3n del vecino se basa en su inestabilidad
                instability_score = neighbor_data['inestabilidad_turchin']['valor']
                # Se suma un 20% de su inestabilidad como presi\u00f3n
                total_pressure += instability_score * 0.2
        
        # Promedio de la presi\u00f3n de todos los pa\u00edses fronterizos
        if borders:
            return total_pressure / len(borders)
        return 0.0

    def save_to_json(self, data: List[Dict], filename: str = 'data/indices_paises_procesado.json'):
        """Guarda los datos procesados en un archivo JSON."""
        file_path = os.path.join(self.DATA_DIR, filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logging.info(f"Datos guardados en {file_path}")

    def main(self):
        """Proceso principal de carga de datos desde archivos JSON."""
        start_time = time.time()
        
        logging.info("Iniciando el proceso de carga de datos desde archivos JSON.")
        
        # Cargar datos procesados del archivo data_paises.json
        file_name = 'data_paises.json'
        paises_data = self._load_json_data(file_name)
        if not paises_data or 'results' not in paises_data:
            logging.error("No se pudieron cargar los datos de data_paises.json. El proceso finaliza.")
            return

        initial_results = {item['country_code']: item for item in paises_data['results']}
        
        logging.info("Iniciando el segundo pase - Rec\u00e1lculo de la presi\u00f3n fronteriza e inestabilidad final")
        
        final_results = []
        for country_code in self.country_codes:
            if country_code in initial_results:
                result = initial_results[country_code]
                
                # Recalcular la presi\u00f3n fronteriza y la inestabilidad si los datos est\u00e1n disponibles
                border_pressure = self.calculate_border_pressure(country_code, initial_results)
                final_instability = self.calculate_turchin_instability(result['indicators'], border_pressure)
                
                # Actualizar los valores en el resultado
                result['inestabilidad_turchin'] = final_instability
                result['border_pressure'] = round(border_pressure, 2)
                final_results.append(result)
                logging.info(f"Inestabilidad final para {country_code} recalculada con presi\u00f3n fronteriza: {final_instability['valor']}")
        
        self.save_to_json(final_results)
        logging.info(f"Proceso principal completado en {time.time() - start_time:.2f} segundos")

if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)
    processor = CliodynamicDataProcessor()
    processor.main()

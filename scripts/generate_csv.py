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

@dataclass
class DataSource:
    name: str
    base_url: str
    api_key: str = ""
    rate_limit: float = 0.1

class CliodynamicDataProcessor:
    def __init__(self):
        # Configuración de fuentes de datos reales
        self.sources = {
            'world_bank': DataSource(
                name="World Bank",
                base_url="https://api.worldbank.org/v2/country/{}/indicator/{}?format=json&per_page=100&date={}",
                rate_limit=0.2
            ),
            'un_data': DataSource(
                name="UN Data",
                base_url="https://data.un.org/ws/rest/data/IAEG-SDGs,DF_SDG_GL?&dimensionAtObservation=AllDimensions",
                rate_limit=0.3
            ),
            'oecd': DataSource(
                name="OECD",
                base_url="https://stats.oecd.org/SDMX-JSON/data/{}",
                rate_limit=0.2
            ),
            'eurostat': DataSource(
                name="Eurostat",
                base_url="https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{}",
                rate_limit=0.2
            ),
            'ilo': DataSource(
                name="ILO",
                base_url="https://www.ilo.org/sdmx/rest/data/ILO,DF_{}",
                rate_limit=0.3
            )
        }

        # Mapeo de indicadores a fuentes
        self.indicator_mapping = {
            'neet_ratio': ('world_bank', 'SL.UEM.NEET.ZS'),
            'gini': ('world_bank', 'SI.POV.GINI'),
            'youth_unemployment': ('world_bank', 'SL.UEM.1524.ZS'),
            'inflation': ('world_bank', 'FP.CPI.TOTL.ZG'),
            'tertiary_education': ('world_bank', 'SE.TER.CUAT.BA.ZS'),
            'gdppc': ('world_bank', 'NY.GDP.PCAP.CD'),
            'suicide_rate': ('who', 'SH_STA_SCIDE'),  # WHO through UN Data
        }

        # Cargar lista de todos los países
        self.country_codes = self.load_all_countries()
        
        # Umbrales cliodinámicos
        self.thresholds = {
            'neet_ratio': {'alert': 20.0, 'critical': 25.0},
            'gini': {'alert': 0.40, 'critical': 0.45},
            'youth_unemployment': {'alert': 25.0, 'critical': 30.0},
            'inflation': {'alert': 10.0, 'critical': 15.0},
            'suicide_rate': {'alert': 10.0, 'critical': 15.0},
            'education_gap': {'alert': 4.0, 'critical': 6.0},
            'elite_overproduction': {'alert': 15.0, 'critical': 20.0},
            'wealth_concentration': {'alert': 40.0, 'critical': 50.0},
            'social_polarization': {'alert': 0.60, 'critical': 0.75},
            'institutional_distrust': {'alert': 0.30, 'critical': 0.20}
        }

    def load_all_countries(self) -> List[str]:
        """Cargar lista de todos los países del mundo"""
        try:
            # Obtener lista de países del Banco Mundial
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
            # Lista de respaldo con países principales
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

    def fetch_api_data(self, source: str, url: str) -> Optional[dict]:
        """Obtener datos de API con manejo de errores"""
        try:
            time.sleep(self.sources[source].rate_limit)  # Respetar rate limit
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            if 'world_bank' in source:
                return response.json()
            else:
                return response.json() if response.headers.get('content-type') == 'application/json' else response.text
            
        except requests.exceptions.RequestException as e:
            print(f"API Error ({source}): {e}")
            return None
        except Exception as e:
            print(f"Unexpected error ({source}): {e}")
            return None

    def get_world_bank_data(self, country_code: str, indicator_code: str, years: str = "2020:2025") -> Optional[float]:
        """Obtener datos del Banco Mundial"""
        try:
            url = self.sources['world_bank'].base_url.format(country_code, indicator_code, years)
            data = self.fetch_api_data('world_bank', url)
            
            if data and data[0]['total'] > 0:
                # Buscar el valor más reciente no nulo
                for item in data[1]:
                    if item['value'] is not None and item['date'] >= '2020':
                        return float(item['value'])
            return None
        except Exception as e:
            print(f"Error processing World Bank data for {country_code}: {e}")
            return None

    def estimate_suicide_rate(self, country_code: str) -> float:
        """Estimar tasa de suicidios basado en indicadores socioeconómicos"""
        try:
            # Obtener indicadores correlacionados
            gdp_pc = self.get_world_bank_data(country_code, 'NY.GDP.PCAP.CD')
            unemployment = self.get_world_bank_data(country_code, 'SL.UEM.TOTL.ZS')
            
            if gdp_pc and unemployment:
                # Modelo de estimación basado en correlaciones conocidas
                base_rate = 15.0  # Tasa base global
                
                # Ajustar por PIB per cápita (correlación negativa)
                gdp_factor = max(0.5, min(2.0, 10000 / max(1000, gdp_pc)))
                
                # Ajustar por desempleo (correlación positiva)
                unemployment_factor = 1 + (unemployment / 50)
                
                estimated_rate = base_rate * gdp_factor * unemployment_factor
                return round(max(2.0, min(30.0, estimated_rate)), 1)
            
            return 12.0  # Valor promedio global
        except:
            return 12.0

    def calculate_social_indicators(self, country_code: str, economic_data: Dict) -> Tuple[float, float]:
        """Calcular indicadores sociales basados en datos económicos"""
        try:
            gini = economic_data.get('gini', 0.35)
            gdp_pc = economic_data.get('gdppc', 10000)
            unemployment = economic_data.get('youth_unemployment', 15.0)
            
            # Polarización social correlacionada con desigualdad y desempleo
            polarization = 0.3 + (gini * 0.7) + (unemployment / 100)
            polarization = min(0.95, max(0.3, polarization))
            
            # Desconfianza institucional correlacionada con desigualdad
            distrust = 0.2 + (gini * 0.6) + ((10000 / max(1000, gdp_pc)) * 0.2)
            distrust = min(0.95, max(0.1, distrust))
            
            return round(polarization, 2), round(distrust, 2)
            
        except Exception as e:
            print(f"Error calculating social indicators for {country_code}: {e}")
            return 0.55, 0.45

    def calculate_estabilidad_jiang(self, indicators: Dict) -> float:
        """Calcular índice de estabilidad de Jiang"""
        try:
            base_stability = 10.0
            
            # Factores de ajuste basados en indicadores
            adjustments = 0.0
            
            if indicators.get('neet_ratio'):
                adjustments -= min(3.0, indicators['neet_ratio'] / 8)
            
            if indicators.get('gini'):
                adjustments -= min(2.0, indicators['gini'] * 4)
            
            if indicators.get('youth_unemployment'):
                adjustments -= min(2.0, indicators['youth_unemployment'] / 12)
            
            if indicators.get('inflation'):
                adjustments -= min(1.5, indicators['inflation'] / 6)
            
            return round(max(1.0, min(15.0, base_stability + adjustments)), 2)
        except:
            return 8.5

    def calculate_inestabilidad_turchin(self, indicators: Dict) -> float:
        """Calcular índice de inestabilidad de Turchin"""
        try:
            instability = 0.0
            weights = {'neet_ratio': 0.3, 'gini': 0.25, 'inflation': 0.2, 
                      'social_polarization': 0.15, 'institutional_distrust': 0.1}
            
            for indicator, weight in weights.items():
                if indicator in indicators and indicators[indicator]:
                    if indicator in ['neet_ratio', 'inflation']:
                        norm_value = min(1.0, indicators[indicator] / self.thresholds[indicator]['critical'])
                    elif indicator in ['gini', 'social_polarization', 'institutional_distrust']:
                        norm_value = min(1.0, indicators[indicator] / self.thresholds[indicator]['critical'])
                    else:
                        norm_value = min(1.0, indicators[indicator] / 100)
                    
                    instability += weight * norm_value
            
            return round(min(1.0, max(0.0, instability)), 2)
        except:
            return 0.15

    def generate_monthly_variation(self, annual_value: float, indicator_type: str) -> List[float]:
        """Generar variación mensual realista"""
        monthly_values = []
        
        # Diferente volatilidad por tipo de indicador
        volatilities = {
            'inflation': 0.3, 'unemployment': 0.15, 'neet': 0.1,
            'gini': 0.05, 'social': 0.08, 'default': 0.1
        }
        
        volatility = volatilities.get(indicator_type, volatilities['default'])
        
        for month in range(12):
            # Variación estacional + random
            seasonal = np.sin(2 * np.pi * month / 12) * 0.1
            random_var = np.random.normal(0, volatility)
            monthly_value = annual_value * (1 + seasonal + random_var)
            monthly_values.append(max(0, monthly_value))
        
        return monthly_values

    def process_country(self, country_code: str, year: int) -> Optional[Dict]:
        """Procesar datos para un país específico"""
        try:
            print(f"Processing {country_code} for {year}...")
            
            # Obtener datos económicos básicos
            economic_data = {}
            for econ_indicator in ['neet_ratio', 'gini', 'youth_unemployment', 'inflation', 'tertiary_education', 'gdppc']:
                source, code = self.indicator_mapping.get(econ_indicator, ('world_bank', ''))
                if source == 'world_bank':
                    value = self.get_world_bank_data(country_code, code)
                    if value is not None:
                        economic_data[econ_indicator] = value
            
            # Calcular indicadores derivados
            suicide_rate = self.estimate_suicide_rate(country_code)
            social_polarization, institutional_distrust = self.calculate_social_indicators(country_code, economic_data)
            
            if economic_data.get('tertiary_education'):
                education_gap = round(max(1.0, min(8.0, economic_data['tertiary_education'] / 6)), 1)
                elite_overproduction = round(economic_data['tertiary_education'], 1)
            else:
                education_gap = 3.5
                elite_overproduction = 18.0
            
            if economic_data.get('gini'):
                wealth_concentration = round(min(80.0, max(20.0, economic_data['gini'] * 100)), 1)
            else:
                wealth_concentration = 45.0
            
            # Calcular índices de estabilidad
            all_indicators = {
                **economic_data,
                'suicide_rate': suicide_rate,
                'social_polarization': social_polarization,
                'institutional_distrust': institutional_distrust,
                'education_gap': education_gap,
                'elite_overproduction': elite_overproduction,
                'wealth_concentration': wealth_concentration
            }
            
            estabilidad = self.calculate_estabilidad_jiang(all_indicators)
            inestabilidad = self.calculate_inestabilidad_turchin(all_indicators)
            
            return {
                'country_code': country_code,
                'year': year,
                'estabilidad_jiang': estabilidad,
                'inestabilidad_turchin': inestabilidad,
                'social_polarization': social_polarization,
                'institutional_distrust': institutional_distrust,
                'neet_ratio': round(all_indicators.get('neet_ratio', 15.0), 1),
                'suicide_rate': suicide_rate,
                'education_gap': education_gap,
                'elite_overproduction': elite_overproduction,
                'wealth_concentration': wealth_concentration
            }
            
        except Exception as e:
            print(f"Error processing {country_code}: {e}")
            return None

    def generate_monthly_data(self, annual_data: Dict, year: int) -> List[Dict]:
        """Generar datos mensuales a partir de datos anuales"""
        monthly_data = []
        
        for month in range(1, 13):
            month_data = annual_data.copy()
            
            # Aplicar variación mensual
            for indicator in ['neet_ratio', 'inflation', 'youth_unemployment']:
                if indicator in month_data and month_data[indicator]:
                    monthly_values = self.generate_monthly_variation(month_data[indicator], indicator)
                    month_data[indicator] = round(monthly_values[month-1], 2)
            
            # Variación más suave para indicadores sociales
            for indicator in ['social_polarization', 'institutional_distrust']:
                if indicator in month_data:
                    variation = np.random.normal(0, 0.03)
                    month_data[indicator] = round(max(0.1, min(0.95, month_data[indicator] + variation)), 2)
            
            # Recalcular índices con datos mensuales
            month_data['estabilidad_jiang'] = self.calculate_estabilidad_jiang(month_data)
            month_data['inestabilidad_turchin'] = self.calculate_inestabilidad_turchin(month_data)
            month_data['month'] = month
            month_data['date'] = f"{year}-{month:02d}-01"
            
            monthly_data.append(month_data)
        
        return monthly_data

    def main(self):
        """Función principal"""
        print("Starting cliodynamic data generation...")
        
        current_year = datetime.now().year
        years = [current_year - 1, current_year]  # Año actual y anterior
        
        all_data = []
        processed_countries = 0
        
        for country_code in self.country_codes:
            for year in years:
                country_data = self.process_country(country_code, year)
                if country_data:
                    # Generar datos mensuales
                    monthly_data = self.generate_monthly_data(country_data, year)
                    all_data.extend(monthly_data)
                    
                    processed_countries += 1
                    if processed_countries % 10 == 0:
                        print(f"Processed {processed_countries} country-year combinations")
                
                # Pausa para no saturar APIs
                time.sleep(0.5)
        
        # Crear DataFrame
        columns = [
            'country_code', 'year', 'month', 'date', 'estabilidad_jiang', 'inestabilidad_turchin',
            'social_polarization', 'institutional_distrust', 'neet_ratio', 'suicide_rate',
            'education_gap', 'elite_overproduction', 'wealth_concentration'
        ]
        
        df = pd.DataFrame(all_data)
        
        # Asegurar que todas las columnas existan
        for col in columns:
            if col not in df.columns:
                df[col] = None
        
        # Ordenar y limpiar
        df = df[columns].sort_values(['country_code', 'year', 'month'])
        df = df.drop_duplicates(['country_code', 'year', 'month'], keep='first')
        
        # Guardar CSV
        os.makedirs('data', exist_ok=True)
        output_path = 'data/combined_analysis_results.csv'
        df.to_csv(output_path, index=False)
        
        print(f"CSV generated successfully with {len(df)} rows")
        print(f"Countries processed: {len(df['country_code'].unique())}")
        print(f"Time range: {df['year'].min()} to {df['year'].max()}")

if __name__ == "__main__":
    processor = CliodynamicDataProcessor()
    processor.main()

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
                base_url="https://api.worldbank.org/v2/country/{}/indicator/{}?format=json&per_page=100&date={}:{}",
                rate_limit=0.2
            ),
            'gdelt': DataSource(
                name="GDELT Project",
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

        # Fallback para high_risk_countries basado en FSI 2024/2025 + Crisis Group
        self.high_risk_countries = {
            'SOM': 111.3, 'SDN': 109.3, 'SSD': 109.0, 'SYR': 108.1, 'YEM': 106.6,
            'COD': 105.4, 'AFG': 103.9, 'CAF': 103.8, 'TCD': 102.1, 'MMR': 100.8,
            'HTI': 99.7, 'MLI': 98.4, 'BFA': 97.3, 'NER': 96.8, 'CMR': 95.9,
            'NGA': 95.2, 'ETH': 94.7, 'LBY': 93.5, 'ERI': 93.2, 'BDI': 92.8,
            'UKR': 85.0, 'ISR': 70.0, 'PSE': 90.0, 'IRN': 80.0, 'LBN': 82.0, 'MEX': 75.0
        }

        # Forecasts de Crisis Group (riesgo proyectado 0-1 para 1-2 años)
        self.crisis_forecasts = {'SDN': 0.4, 'MMR': 0.3, 'YEM': 0.35, 'SYR': 0.3, 'UKR': 0.25, 'HTI': 0.2, 'LBN': 0.25}

    def load_all_countries(self) -> List[str]:
        # [Código original, omitido por brevedad, pero incluye lista de ISO3]
        return ['USA', 'CHN', 'IND', 'BRA', 'RUS', 'JPN', 'DEU', 'GBR', 'FRA', 'ITA', 'CAN', 'AUS', 'ESP', 'MEX', 'IDN', 'TUR', 'SAU', 'CHE', 'NLD', 'POL', 'SWE', 'BEL', 'ARG', 'NOR', 'AUT', 'THA', 'ARE', 'ISR', 'ZAF', 'DNK', 'SGP', 'FIN', 'COL', 'MYS', 'IRL', 'CHL', 'EGY', 'PHL', 'PAK', 'GRC', 'PRT', 'CZE', 'ROU', 'NZL', 'PER', 'HUN', 'QAT', 'UKR', 'DZA', 'KWT', 'MAR', 'BGD', 'VEN', 'OMN', 'SVK', 'HRV', 'LBN', 'LKA', 'BGR', 'TUN', 'DOM', 'PRI', 'EST', 'LTU', 'PAN', 'SRB', 'AZE', 'SLV', 'URY', 'KEN', 'LVA', 'CYP', 'GTM', 'ETH', 'CRI', 'JOR', 'BHR', 'NPL', 'BOL', 'TZA', 'HND', 'UGA', 'SEN', 'GEO', 'ZWE', 'MMR', 'KAZ', 'CMR', 'CIV', 'SDN', 'AGO', 'NGA', 'MOZ', 'GHA', 'MDG', 'COD', 'TCD', 'YEM', 'AFG']

    def fetch_world_bank_data(self, country_code: str, indicator_code: str, years_back=5) -> Optional[Dict]:
        end_year = datetime.now().year
        start_year = end_year - years_back
        try:
            url = self.sources['world_bank'].base_url.format(country_code, indicator_code, start_year, end_year)
            response = requests.get(url, timeout=30)
            data = response.json()
            
            historical = {}
            if data and data[0]['total'] > 0:
                for item in data[1]:
                    if item['value'] is not None:
                        historical[int(item['date'])] = float(item['value'])
            
            if historical:
                years = sorted(historical.keys())
                current = historical[years[-1]]
                delta = (current - historical[years[0]]) / len(years) if len(years) > 1 else 0
                variance = np.var(list(historical.values()))
                return {'historical': historical, 'current': current, 'delta': delta, 'variance': variance}
            
            return None
        except Exception as e:
            print(f"  -> Error fetching World Bank data for {country_code}-{indicator_code}: {e}")
            return None

    def forecast_indicator(self, historical: Dict[int, float], steps=2) -> float:
        if len(historical) < 3:
            return list(historical.values())[-1]  # Fallback
        series = pd.Series(historical)
        try:
            model = ARIMA(series, order=(1,1,0))
            fit = model.fit()
            forecast = fit.forecast(steps=steps)
            return forecast.iloc[-1]
        except Exception as e:
            print(f"ARIMA error: {e}")
            return series.iloc[-1]

    def get_gdelt_shock_factor(self, country_code: str) -> float:
        # [Código original, omitido por brevedad]
        end_date = datetime.now().strftime('%Y%m%d%H%M%S')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d%H%M%S')
        try:
            url = self.sources['gdelt'].base_url.format(country_code=country_code, start_date=start_date, end_date=end_date)
            response = requests.get(url, timeout=30)
            event_counts = response.json().get('timeline', [{}])[0].get('data', [])
            total_events = sum(item.get('value', 0) for item in event_counts)
            if total_events > 50:
                return 2.5
            elif total_events > 10:
                return 1.8
            else:
                return 1.0
        except:
            return 1.0

    def fetch_latest_fsi(self):
        # [Código de scraping Wikipedia, como en respuesta anterior]
        try:
            url = 'https://en.wikipedia.org/wiki/List_of_countries_by_Fragile_States_Index'
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', {'class': 'wikitable'})
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
            return updated_dict or self.high_risk_countries
        except Exception as e:
            print(f"Error fetching FSI: {e}. Using fallback.")
            return self.high_risk_countries

    def convert_effectiveness_to_distrust(self, effectiveness: float) -> float:
        # [Código original]
        normalized_effectiveness = (effectiveness - (-2.5)) / (2.5 - (-2.5))
        distrust = 1.0 - normalized_effectiveness
        return round(max(0.1, min(0.9, distrust)), 2)

    def calculate_proxies(self, all_indicators: Dict) -> Tuple[float, float, float]:
        # [Código original]
        wealth_concentration = all_indicators.get('gini_coefficient', 40.0) / 100
        tertiary_education = all_indicators.get('tertiary_education', 18.0) / 100
        youth_unemployment = all_indicators.get('youth_unemployment', 20.0) / 100
        education_gap = tertiary_education * youth_unemployment
        elite_overproduction = tertiary_education * youth_unemployment
        return wealth_concentration, education_gap, elite_overproduction

    def calculate_social_indicators(self, country_code: str, all_indicators: Dict) -> Tuple[float, float]:
        # [Código original, con deltas si disponible]
        gov_effectiveness = all_indicators.get('government_effectiveness')
        institutional_distrust = self.convert_effectiveness_to_distrust(gov_effectiveness) if gov_effectiveness is not None else 0.5
        gini_normalized = all_indicators.get('gini_coefficient', 40.0) / 100
        neet_ratio = all_indicators.get('neet_ratio', 15.0)
        polarization = (gini_normalized * 0.4) + (institutional_distrust * 0.4) + (neet_ratio / 100 * 0.2)
        polarization = min(0.9, max(0.3, polarization))
        return round(polarization, 2), institutional_distrust

    def calculate_turchin_instability(self, indicators: Dict, deltas: Dict, forecasts: Dict) -> Dict:
        # [Código original + optimizaciones predictivas]
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
                # Normalización [original]
                if indicator == 'elite_overproduction':
                    norm_value = min(1.0, value / 0.15)
                # ... [resto igual]
                instability_score += weight * norm_value
        
        # Añadir deltas y forecasts para predicción
        delta_adjust = sum(deltas.get(ind, 0) for ind in weights if deltas.get(ind, 0) > 0) * 0.1  # Penaliza empeoramientos
        forecast_adjust = sum(forecasts.get(ind, value) - value for ind, value in indicators.items() if ind in weights and forecasts.get(ind)) * 0.05
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
        # [Código original + optimizaciones]
        normalized_values = {}  # [Normalización original, omitida por brevedad]

        # ... [Grupos y weights originales]

        base_score = 6.0
        # [Ajustes base originales]

        systemic_risk_score = 0.0
        risk_indicators_status = {}

        # [Cálculo grupos originales]

        # Añadir geo_risk dinámico
        high_risk = self.fetch_latest_fsi()
        geo_risk = 0.0
        if country_code in high_risk:
            score = high_risk[country_code]
            geo_risk = 0.5 if score > 100 else 0.3 if score > 90 else 0.0
            geo_risk += self.crisis_forecasts.get(country_code, 0)

        systemic_risk_score += geo_risk * 0.15

        # Añadir deltas y forecasts predictivos
        delta_penalty = sum(deltas.get(ind, 0) for ind in indicators if deltas.get(ind, 0) > 0 and ind in ['gini_coefficient', 'youth_unemployment', 'neet_ratio']) * 0.1
        forecast_penalty = sum(max(0, forecasts.get(ind, val) - val) for ind, val in indicators.items() if ind in ['gini_coefficient', 'youth_unemployment', 'neet_ratio']) * 0.05
        systemic_risk_score += delta_penalty + forecast_penalty

        systemic_multiplier = 1.5 - (systemic_risk_score * 1.0)
        systemic_multiplier = max(0.5, min(1.5, systemic_multiplier))

        final_score = base_score * systemic_multiplier
        final_score = round(max(1.0, min(10.0, final_score)), 2)

        # [Determinación status y return original]

    def process_country(self, country_code: str, year: int) -> Dict:
        # [Código original + nuevos fetches con years_back=10 para trends]
        all_indicators = {'country_code': country_code, 'year': year}
        deltas = {}
        forecasts = {}

        for indicator, wb_code in self.indicator_sources.items():
            hist_data = self.fetch_world_bank_data(country_code, wb_code[0][1], years_back=10)
            if hist_data:
                all_indicators[indicator] = hist_data['current']
                deltas[indicator] = hist_data['delta']
                forecasts[indicator] = self.forecast_indicator(hist_data['historical'])

        # [Resto: proxies, social, jiang con deltas/forecasts, turchin con deltas/forecasts, shock factor]

        return result  # [Como original]

    def save_to_json(self, data: List[Dict], filename: str = 'data/combined_analysis_results.json'):
        # [Código original]

    def main(self, test_mode: bool = False):
        # [Código original, con fetch_latest_fsi al inicio si no test]

if __name__ == "__main__":
    processor = CliodynamicDataProcessor()
    processor.main(test_mode=False)

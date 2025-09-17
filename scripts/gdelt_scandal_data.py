import requests
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any
from bs4 import BeautifulSoup
import pdfplumber  # pip install pdfplumber

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EudaimoniaPredictorGenerator:
    """
    Genera un archivo JSON con índices de corrupción, tensión y predictor de Eudaimonia.
    Incorpora datos históricos para contexto y datos frescos de los últimos 3 meses para predicción.
    Fuentes: Media Cloud (fresco, requiere key), NewsAPI (fresco, requiere key), CPI/GPI (histórico/anual).
    Para todos los países de la lista.
    """
    DATA_DIR = 'data'
    OUTPUT_FILE = os.path.join(DATA_DIR, 'data_indices.json')
    
    # URL para CPI histórico (Wikipedia scraping para lo más reciente)
    CPI_URL = "https://en.wikipedia.org/wiki/Corruption_Perceptions_Index"
    
    # URL para GPI (PDF anual, ajusta año si necesario)
    GPI_URL = "https://www.visionofhumanity.org/wp-content/uploads/2025/06/Global-Peace-Index-2025-web.pdf"
    
    def __init__(self, country_codes: list):
        self.country_codes = country_codes
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=90)  # Últimos 3 meses para datos frescos
        self.country_names = {  # Mapeo ISO3 to name for queries
            'USA': 'United States',
            'CHN': 'China',
            'IND': 'India',
            'BRA': 'Brazil',
            'RUS': 'Russia',
            'JPN': 'Japan',
            'DEU': 'Germany',
            'GBR': 'United Kingdom',
            'CAN': 'Canada',
            'FRA': 'France',
            'ITA': 'Italy',
            'AUS': 'Australia',
            'MEX': 'Mexico',
            'KOR': 'South Korea',
            'SAU': 'Saudi Arabia',
            'TUR': 'Turkey',
            'EGY': 'Egypt',
            'NGA': 'Nigeria',
            'PAK': 'Pakistan',
            'IDN': 'Indonesia',
            'VNM': 'Vietnam',
            'PHL': 'Philippines',
            'ARG': 'Argentina',
            'COL': 'Colombia',
            'POL': 'Poland',
            'ESP': 'Spain',
            'IRN': 'Iran',
            'ZAF': 'South Africa',
            'UKR': 'Ukraine',
            'THA': 'Thailand',
            'VEN': 'Venezuela',
            'CHL': 'Chile',
            'PER': 'Peru',
            'MYS': 'Malaysia',
            'ROU': 'Romania',
            'SWE': 'Sweden',
            'BEL': 'Belgium',
            'NLD': 'Netherlands',
            'GRC': 'Greece',
            'CZE': 'Czech Republic',
            'PRT': 'Portugal',
            'DNK': 'Denmark',
            'FIN': 'Finland',
            'NOR': 'Norway',
            'SGP': 'Singapore',
            'AUT': 'Austria',
            'CHE': 'Switzerland',
            'IRL': 'Ireland',
            'NZL': 'New Zealand',
            'HKG': 'Hong Kong',
            'ISR': 'Israel',
            'ARE': 'United Arab Emirates'
        }
        # Términos para corrupción y tensión (frescos)
        self.corruption_terms = 'corruption OR scandal OR bribery OR "money laundering" OR "abuse of power"'
        self.tension_terms = 'protest OR unrest OR violence OR conflict OR crisis'
        
    def _fetch_historical_cpi(self) -> Dict[str, Any]:
        """
        Fetch CPI histórico/más reciente via scraping Wikipedia.
        """
        try:
            response = requests.get(self.CPI_URL)
            soup = BeautifulSoup(response.text, 'lxml')
            table = soup.find('table', {'class': 'wikitable sortable'})
            cpi_data = {}
            if table:
                for row in table.find_all('tr')[1:]:
                    cells = row.find_all('td')
                    if len(cells) >= 3:
                        rank = cells[0].text.strip()
                        country = cells[1].text.strip()
                        score = cells[2].text.strip()
                        cpi_data[country] = {'score': int(score) if score.isdigit() else None, 'rank': int(rank) if rank.isdigit() else None}
            logging.info("✅ CPI histórico fetched")
            return cpi_data
        except Exception as e:
            logging.error(f"❌ Error fetching CPI: {e}")
            return {}

    def _fetch_historical_gpi(self) -> Dict[str, Any]:
        """
        Fetch GPI histórico/más reciente via PDF parsing.
        """
        try:
            response = requests.get(self.GPI_URL, stream=True)
            with open('temp_gpi.pdf', 'wb') as f:
                f.write(response.content)
            gpi_data = {}
            with pdfplumber.open('temp_gpi.pdf') as pdf:
                for page in pdf.pages[10:20]:  # Ajusta páginas donde está la tabla (inspecciona PDF)
                    table = page.extract_table()
                    if table:
                        for row in table[1:]:
                            if len(row) >= 3:
                                country = row[0].strip()
                                score = float(row[1].strip()) if row[1].strip() else None
                                rank = int(row[2].strip()) if row[2].strip().isdigit() else None
                                gpi_data[country] = {'score': score, 'rank': rank}
            os.remove('temp_gpi.pdf')
            logging.info("✅ GPI histórico fetched")
            return gpi_data
        except Exception as e:
            logging.error(f"❌ Error fetching GPI: {e}")
            return {}

    def _fetch_fresh_media_counts(self, country_name: str, query: str, media_cloud_key: str = None, newsapi_key: str = None) -> int:
        """
        Fetch conteos frescos de menciones en medios usando Media Cloud o NewsAPI.
        """
        count = 0
        if media_cloud_key:
            # Media Cloud (preferido para fresco)
            params = {
                'q': f'{query} AND country:"{country_name}"',
                'start_date': self.start_date.strftime('%Y-%m-%d'),
                'end_date': self.end_date.strftime('%Y-%m-%d')
            }
            headers = {'Authorization': f'Bearer {media_cloud_key}'}
            response = requests.get('https://api.mediacloud.org/api/v2/stories/count', params=params, headers=headers)
            count = response.json().get('count', 0) if response.ok else 0
        elif newsapi_key:
            # Fallback a NewsAPI para fresco
            url = 'https://newsapi.org/v2/everything'
            params = {
                'q': f'{query} {country_name}',
                'from': self.start_date.strftime('%Y-%m-%d'),
                'to': self.end_date.strftime('%Y-%m-%d'),
                'apiKey': newsapi_key,
                'sortBy': 'publishedAt'
            }
            response = requests.get(url, params=params)
            count = response.json().get('totalResults', 0) if response.ok else 0
        return count

    def generate_indices_json(self, media_cloud_key: str = None, newsapi_key: str = None):
        """
        Genera JSON con datos históricos (contexto) y frescos (predicción).
        Calcula índices y predictor de Eudaimonia.
        """
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR)

        historical_cpi = self._fetch_historical_cpi()
        historical_gpi = self._fetch_historical_gpi()
        all_data = {}

        for code in self.country_codes:
            name = self.country_names.get(code, code)
            
            # Histórico para contexto
            hist_cpi_score = historical_cpi.get(name, {}).get('score')
            hist_gpi_score = historical_gpi.get(name, {}).get('score')
            hist_corruption = 100 - hist_cpi_score if hist_cpi_score else None
            hist_tension = hist_gpi_score if hist_gpi_score else None
            
            # Fresco para predicción
            fresh_corruption_count = self._fetch_fresh_media_counts(name, self.corruption_terms, media_cloud_key, newsapi_key)
            fresh_total_count = self._fetch_fresh_media_counts(name, '*', media_cloud_key, newsapi_key)  # Total stories
            fresh_corruption_index = (fresh_corruption_count / fresh_total_count * 100) if fresh_total_count > 0 else 0
            
            fresh_tension_count = self._fetch_fresh_media_counts(name, self.tension_terms, media_cloud_key, newsapi_key)
            fresh_tension_index = (fresh_tension_count / fresh_total_count * 100) if fresh_total_count > 0 else 0
            
            # Predictor de Eudaimonia (usando fresco, fallback histórico si 0)
            if fresh_corruption_index == 0 and hist_corruption:
                norm_cor = hist_corruption / 100
            else:
                norm_cor = fresh_corruption_index / 100
            if fresh_tension_index == 0 and hist_tension:
                norm_ten = (hist_tension - 1) / 2.5 if hist_tension else 0
            else:
                norm_ten = fresh_tension_index / 100  # Normalize tension to 0-1 (assuming % as proxy)
            eudaimonia_predictor = 100 - (((norm_cor + norm_ten) / 2) * 100)
            eudaimonia_predictor = round(eudaimonia_predictor, 4)
            
            all_data[code] = {
                "historical": {
                    "corruption_index": hist_corruption,
                    "tension_index": hist_tension
                },
                "fresh": {
                    "corruption_index": round(fresh_corruption_index, 4),
                    "tension_index": round(fresh_tension_index, 4)
                },
                "eudaimonia_predictor": eudaimonia_predictor,
                "data_source": "Media Cloud/NewsAPI (fresh) + CPI/GPI (historical)"
            }
        
        final_data = {
            "metadata": {
                "purpose": "Predictors for Eudaimonia with historical context and fresh data",
                "processing_date": datetime.now().isoformat(),
                "time_range_fresh": f"{self.start_date.date()} to {self.end_date.date()}",
                "time_range_historical": "2024-2025 annual data"
            },
            "results": all_data
        }
        
        with open(self.OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)
        
        logging.info(f"✅ Datos guardados en {self.OUTPUT_FILE}")

if __name__ == "__main__":
    country_list = [
        'USA', 'CHN', 'IND', 'BRA', 'RUS', 'JPN', 'DEU', 'GBR', 'CAN', 'FRA',
        'ITA', 'AUS', 'MEX', 'KOR', 'SAU', 'TUR', 'EGY', 'NGA', 'PAK', 'IDN',
        'VNM', 'PHL', 'ARG', 'COL', 'POL', 'ESP', 'IRN', 'ZAF', 'UKR', 'THA',
        'VEN', 'CHL', 'PER', 'MYS', 'ROU', 'SWE', 'BEL', 'NLD', 'GRC', 'CZE',
        'PRT', 'DNK', 'FIN', 'NOR', 'SGP', 'AUT', 'CHE', 'IRL', 'NZL', 'HKG',
        'ISR', 'ARE'
    ]
    
    generator = EudaimoniaPredictorGenerator(country_list)
    # Proporciona keys para datos frescos
    generator.generate_indices_json(media_cloud_key='TU_MEDIA_CLOUD_KEY', newsapi_key='TU_NEWSAPI_KEY')  # Reemplaza con tus keys o None para solo histórico

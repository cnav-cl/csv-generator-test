import requests
import json
from datetime import datetime, timedelta
from typing import Dict, Any
import logging

# Configuración de logging
logging.basicBasic(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EudaimoniaPredictorGenerator:
    """
    Genera un archivo JSON con índices de corrupción, tensión y predictor de Eudaimonia basados en datos frescos de CPI y GPI.
    Para datos frescos, se consulta las fuentes oficiales via API o scraping simple. Nota: Para Media Cloud, requiere API key y tag ids; aquí usamos fuentes públicas.
    """
    DATA_DIR = 'data'
    OUTPUT_FILE = os.path.join(DATA_DIR, 'data_indices.json')
    
    def __init__(self, country_codes: list):
        self.country_codes = country_codes
        self.country_names = {  # Mapeo ISO3 to name for GPI
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
        
    def _fetch_cpi_data(self):
        # Fetch from Wikipedia or Transparency, here example with requests and beautifulsoup
        # Note: Install beautifulsoup4 if needed: pip install beautifulsoup4 lxml
        from bs4 import BeautifulSoup
        url = "https://en.wikipedia.org/wiki/Corruption_Perceptions_Index"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')
        # Find the table for 2024, parse rows
        # This is pseudo-code; adjust to actual table class/id
        table = soup.find('table', {'class': 'wikitable'})
        cpi = {}
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) > 2:
                country = cells[0].text.strip()
                score = int(cells[1].text.strip())
                rank = int(cells[2].text.strip())
                cpi[country] = {'score': score, 'rank': rank}
        return cpi
    
    def _fetch_gpi_data(self):
        # Fetch from visionofhumanity map, but since JS, alternatively download PDF and parse, or use API if available
        # For simplicity, use a static URL or assume CSV
        # Alternative: Use Media Cloud for custom, with API key
        # Here, pseudo for GPI
        url = "https://www.visionofhumanity.org/wp-content/uploads/2025/06/Global-Peace-Index-2025-web.pdf"
        # To parse PDF, install pdfplumber: pip install pdfplumber
        import pdfplumber
        with requests.get(url, stream=True) as r:
            with open('gpi.pdf', 'wb') as f:
                f.write(r.content)
        with pdfplumber.open('gpi.pdf') as pdf:
            # Parse pages with table, adjust page numbers
            gpi = {}
            for page in pdf.pages:
                table = page.extract_table()
                for row in table[1:]:  # Skip header
                    country = row[0]
                    score = float(row[1])
                    rank = int(row[2])
                    gpi[country] = {'score': score, 'rank': rank}
        return gpi
    
    def generate_indices_json(self, media_cloud_api_key: str = None):
        """
        Genera el JSON con los índices.
        Para Media Cloud, si se proporciona API key, usar para datos frescos de menciones.
        """
        if media_cloud_api_key:
            # Use Media Cloud for fresh media mentions
            # First, need tag ids for countries. Assume user has a dict or fetch
            country_tag_ids = {  # Example, user to fill or fetch
                'USA': 34412234,  # Example for US
                # Add for others, from https://sources.mediacloud.org/#collections/countries
            }
            all_data = {}
            for code in self.country_codes:
                tag_id = country_tag_ids.get(code)
                if tag_id:
                    # Query for corruption mentions
                    query_corruption = 'corruption OR scandal OR bribery OR "money laundering" OR "abuse of power"'
                    params = {
                        'q': query_corruption,
                        'tags_id': tag_id,
                        'start_date': (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'),
                        'end_date': datetime.now().strftime('%Y-%m-%d')
                    }
                    headers = {'Authorization': f'Bearer {media_cloud_api_key}'}
                    response = requests.get('https://api.mediacloud.org/api/v2/stories/count', params=params, headers=headers)
                    corruption_count = response.json().get('count', 0)
                    
                    # Total stories for normalization
                    params['q'] = '*'
                    response = requests.get('https://api.mediacloud.org/api/v2/stories/count', params=params, headers=headers)
                    total_count = response.json().get('count', 0)
                    
                    corruption_percentage = (corruption_count / total_count * 100) if total_count > 0 else 0
                    
                    # Similar for tension
                    query_tension = 'protest OR unrest OR violence OR conflict OR crisis'
                    params['q'] = query_tension
                    response = requests.get('https://api.mediacloud.org/api/v2/stories/count', params=params, headers=headers)
                    tension_count = response.json().get('count', 0)
                    tension_percentage = (tension_count / total_count * 100) if total_count > 0 else 0
                    
                    # Eudaimonia predictor as inverse average
                    avg = (corruption_percentage + tension_percentage) / 2
                    eudaimonia = 100 - avg
                    
                    all_data[code] = {
                        "corruption_index": round(corruption_percentage, 4),
                        "tension_index": round(tension_percentage, 4),
                        "eudaimonia_predictor": round(eudaimonia, 4),
                        "data_source": "Media Cloud"
                    }
                else:
                    logging.warning(f"Tag ID not found for {code}")
        else:
            # Use CPI and GPI as fallback
            cpi = self._fetch_cpi_data()
            gpi = self._fetch_gpi_data()
            all_data = {}
            for code in self.country_codes:
                name = self.country_names.get(code, code)
                cpi_score = cpi.get(name, {}).get('score')
                gpi_score = gpi.get(name, {}).get('score')
                corruption_index = 100 - cpi_score if cpi_score else None
                tension_index = gpi_score if gpi_score else None
                if corruption_index is not None and tension_index is not None:
                    norm_cor = corruption_index / 100
                    norm_ten = (tension_index - 1) / 2.5
                    eudaimonia = 100 - (((norm_cor + norm_ten) / 2) * 100)
                    eudaimonia = round(eudaimonia, 2)
                else:
                    eudaimonia = None
                all_data[code] = {
                    "corruption_index": corruption_index,
                    "tension_index": tension_index,
                    "eudaimonia_predictor": eudaimonia,
                    "data_source": "CPI and GPI"
                }
        
        final_data = {
            "metadata": {
                "source": "Media Cloud or CPI/GPI",
                "purpose": "Predictors for Eudaimonia",
                "processing_date": datetime.now().isoformat(),
                "time_range": "Last 3 months for Media Cloud or 2024-2025 for CPI/GPI"
            },
            "results": all_data
        }
        
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR)
        
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
    # Para Media Cloud, pasa tu API key
    generator.generate_indices_json(media_cloud_api_key='510d87d1bd34bab035ce9b4d5d12ca2e343a078c')  # Reemplaza con tu key o None para fallback

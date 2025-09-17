import requests
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from bs4 import BeautifulSoup
import pdfplumber
import time

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
    OUTPUT_FILE = os.path.join(DATA_DIR, 'data_indices_eudaimonia.json')
    
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

    def _fetch_media_cloud_count(self, country_name: str, query: str, media_cloud_key: str) -> Optional[int]:
        """
        Fetch conteos frescos de menciones en medios usando Media Cloud API.
        Según la documentación oficial: https://github.com/mediacloud/api-tutorial-notebooks
        """
        try:
            # Formato correcto según documentación de Media Cloud
            params = {
                'q': query,
                'fq': f'publish_date:[{self.start_date.strftime("%Y-%m-%d")}T00:00:00Z TO {self.end_date.strftime("%Y-%m-%d")}T23:59:59Z] AND (tags_id_media:1 OR tags_id_media:2)'
            }
            
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Token {media_cloud_key}'
            }
            
            # Pequeña pausa para evitar rate limiting
            time.sleep(0.1)
            
            response = requests.get(
                'https://api.mediacloud.org/api/v2/stories_public/count',
                params=params,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                count = data.get('count', 0)
                logging.info(f"✅ Media Cloud query for {country_name}: {query} -> {count} results")
                return count
            else:
                logging.warning(f"⚠️ Media Cloud API error for {country_name}: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            logging.error(f"❌ Timeout fetching Media Cloud data for {country_name}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Request error fetching Media Cloud data for {country_name}: {e}")
            return None
        except Exception as e:
            logging.error(f"❌ Unexpected error with Media Cloud API for {country_name}: {e}")
            return None

    def _fetch_newsapi_count(self, country_name: str, query: str, newsapi_key: str) -> Optional[int]:
        """
        Fetch conteos frescos de menciones en medios usando NewsAPI.
        """
        try:
            url = 'https://newsapi.org/v2/everything'
            params = {
                'q': f'{query} {country_name}',
                'from': self.start_date.strftime('%Y-%m-%d'),
                'to': self.end_date.strftime('%Y-%m-%d'),
                'apiKey': newsapi_key,
                'sortBy': 'publishedAt',
                'pageSize': 1  # Solo necesitamos el total, no los artículos
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                count = data.get('totalResults', 0)
                logging.info(f"✅ NewsAPI query for {country_name}: {query} -> {count} results")
                return count
            else:
                logging.warning(f"⚠️ NewsAPI error for {country_name}: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            logging.error(f"❌ Timeout fetching NewsAPI data for {country_name}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Request error fetching NewsAPI data for {country_name}: {e}")
            return None
        except Exception as e:
            logging.error(f"❌ Unexpected error with NewsAPI for {country_name}: {e}")
            return None

    def _fetch_fresh_media_counts(self, country_name: str, query: str, media_cloud_key: str = None, newsapi_key: str = None) -> int:
        """
        Fetch conteos frescos de menciones en medios usando Media Cloud o NewsAPI.
        Implementa fallback estratégico entre fuentes.
        """
        count = 0
        
        # Primero intentar con Media Cloud si tenemos key
        if media_cloud_key:
            media_cloud_count = self._fetch_media_cloud_count(country_name, query, media_cloud_key)
            if media_cloud_count is not None:
                return media_cloud_count
            else:
                logging.warning(f"⚠️ Media Cloud failed for {country_name}, trying NewsAPI")
        
        # Fallback a NewsAPI si Media Cloud falla o no tenemos key
        if newsapi_key:
            newsapi_count = self._fetch_newsapi_count(country_name, query, newsapi_key)
            if newsapi_count is not None:
                return newsapi_count
        
        # Si ambas APIs fallan, registrar advertencia pero continuar
        logging.warning(f"⚠️ Both Media Cloud and NewsAPI failed for {country_name}, using fallback value 0")
        return 0

    def generate_indices_json(self, media_cloud_key: str = None, newsapi_key: str = None):
        """
        Genera JSON con datos históricos (contexto) y frescos (predicción).
        Calcula índices y predictor de Eudaimonia.
        """
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR)

        logging.info("🔄 Starting data collection process...")
        
        # Obtener datos históricos
        historical_cpi = self._fetch_historical_cpi()
        historical_gpi = self._fetch_historical_gpi()
        all_data = {}

        # Verificar si tenemos al menos una API key para datos frescos
        has_fresh_data_source = media_cloud_key or newsapi_key
        if not has_fresh_data_source:
            logging.warning("⚠️ No API keys provided, will only use historical data")

        for code in self.country_codes:
            name = self.country_names.get(code, code)
            logging.info(f"📊 Processing data for {name} ({code})...")
            
            # Histórico para contexto
            hist_cpi_score = historical_cpi.get(name, {}).get('score')
            hist_gpi_score = historical_gpi.get(name, {}).get('score')
            hist_corruption = 100 - hist_cpi_score if hist_cpi_score else None
            hist_tension = hist_gpi_score if hist_gpi_score else None
            
            # Inicializar valores frescos
            fresh_corruption_index = 0
            fresh_tension_index = 0
            
            # Fresco para predicción (solo si tenemos API keys)
            if has_fresh_data_source:
                try:
                    fresh_corruption_count = self._fetch_fresh_media_counts(name, self.corruption_terms, media_cloud_key, newsapi_key)
                    fresh_total_count = self._fetch_fresh_media_counts(name, '*', media_cloud_key, newsapi_key)  # Total stories
                    
                    if fresh_total_count > 0:
                        fresh_corruption_index = (fresh_corruption_count / fresh_total_count * 100)
                    
                    fresh_tension_count = self._fetch_fresh_media_counts(name, self.tension_terms, media_cloud_key, newsapi_key)
                    
                    if fresh_total_count > 0:
                        fresh_tension_index = (fresh_tension_count / fresh_total_count * 100)
                        
                except Exception as e:
                    logging.error(f"❌ Error processing fresh data for {name}: {e}")
                    # Continuar con valores por defecto si hay error
            else:
                logging.info(f"ℹ️  Skipping fresh data for {name} (no API keys)")
            
            # Predictor de Eudaimonia (usando fresco, fallback histórico si 0)
            if fresh_corruption_index == 0 and hist_corruption:
                norm_cor = hist_corruption / 100
            else:
                norm_cor = fresh_corruption_index / 100
                
            if fresh_tension_index == 0 and hist_tension:
                norm_ten = (hist_tension - 1) / 2.5 if hist_tension else 0
            else:
                norm_ten = fresh_tension_index / 100
                
            eudaimonia_predictor = 100 - (((norm_cor + norm_ten) / 2) * 100)
            eudaimonia_predictor = round(max(0, min(100, eudaimonia_predictor)), 2)  # Asegurar entre 0-100
            
            all_data[code] = {
                "historical": {
                    "corruption_index": hist_corruption,
                    "tension_index": hist_tension
                },
                "fresh": {
                    "corruption_index": round(fresh_corruption_index, 2),
                    "tension_index": round(fresh_tension_index, 2),
                    "data_available": has_fresh_data_source and fresh_corruption_index + fresh_tension_index > 0
                },
                "eudaimonia_predictor": eudaimonia_predictor,
                "data_source": "Media Cloud/NewsAPI (fresh) + CPI/GPI (historical)" if has_fresh_data_source else "CPI/GPI (historical only)"
            }
        
        final_data = {
            "metadata": {
                "purpose": "Predictors for Eudaimonia with historical context and fresh data",
                "processing_date": datetime.now().isoformat(),
                "time_range_fresh": f"{self.start_date.date()} to {self.end_date.date()}",
                "time_range_historical": "2024-2025 annual data",
                "fresh_data_available": has_fresh_data_source,
                "countries_processed": len(all_data)
            },
            "results": all_data
        }
        
        with open(self.OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)
        
        logging.info(f"✅ Datos guardados en {self.OUTPUT_FILE}")
        return final_data

if __name__ == "__main__":
    country_list = [
        'USA', 'CHN', 'IND', 'BRA', 'RUS', 'JPN', 'DEU', 'GBR', 'CAN', 'FRA',
        'ITA', 'AUS', 'MEX', 'KOR', 'SAU', 'TUR', 'EGY', 'NGA', 'PAK', 'IDN',
        'VNM', 'PHL', 'ARG', 'COL', 'POL', 'ESP', 'IRN', 'ZAF', 'UKR', 'THA',
        'VEN', 'CHL', 'PER', 'MYS', 'ROU', 'SWE', 'BEL', 'NLD', 'GRC', 'CZE',
        'PRT', 'DNK', 'FIN', 'NOR', 'SGP', 'AUT', 'CHE', 'IRL', 'NZL', 'HKG',
        'ISR', 'ARE'
    ]
    
    # Obtener API keys de variables de entorno
    media_cloud_key = os.environ.get('510d87d1bd34bab035ce9b4d5d12ca2e343a078c')
    newsapi_key = os.environ.get('pub_5e95265aba1e4044863ba62e82d7d44e')
    
    generator = EudaimoniaPredictorGenerator(country_list)
    result = generator.generate_indices_json(
        media_cloud_key=media_cloud_key, 
        newsapi_key=newsapi_key
    )
    
    # Resumen de procesamiento
    countries_with_fresh_data = sum(1 for code, data in result['results'].items() 
                                  if data['fresh']['data_available'])
    
    logging.info(f"📈 Procesamiento completado:")
    logging.info(f"   - Países procesados: {len(result['results'])}")
    logging.info(f"   - Países con datos frescos: {countries_with_fresh_data}")
    logging.info(f"   - Datos frescos disponibles: {result['metadata']['fresh_data_available']}")

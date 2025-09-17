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
        # Nuevo: Rango de fechas para el día que terminó hace 48 horas (2 días de retraso)
        self.end_date = datetime.utcnow() - timedelta(days=2)
        self.start_date = self.end_date
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
                                try:
                                    score = float(row[1].strip()) if row[1].strip() else None
                                    rank = int(row[2].strip()) if row[2].strip().isdigit() else None
                                    gpi_data[country] = {'score': score, 'rank': rank}
                                except (ValueError, IndexError):
                                    # Esto ignorará las filas que no tienen un número en la columna del score,
                                    # como las que solo tienen nombres de países.
                                    continue
            os.remove('temp_gpi.pdf')
            logging.info("✅ GPI histórico fetched")
            return gpi_data
        except Exception as e:
            logging.error(f"❌ Error fetching GPI: {e}")
            return {}

    def _fetch_media_cloud_articles(self, country_name: str, query: str, media_cloud_key: str) -> Optional[list]:
        """
        Fetch artículos frescos usando Media Cloud API y devuelve la lista de artículos.
        """
        try:
            params = {
                'q': query,
                'fq': f'publish_date:[{self.start_date.strftime("%Y-%m-%d")}T00:00:00Z TO {self.end_date.strftime("%Y-%m-%d")}T23:59:59Z] AND (tags_id_media:1 OR tags_id_media:2)'
            }
            
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Token {media_cloud_key}'
            }
            
            response = requests.get(
                'https://api.mediacloud.org/api/v2/stories_public',
                params=params,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                articles = data.get('stories', [])
                logging.info(f"✅ Media Cloud query for {country_name}: {query} -> {len(articles)} articles")
                return articles
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

    def _fetch_newsapi_articles(self, country_name: str, query: str, newsapi_key: str) -> Optional[list]:
        """
        Fetch artículos frescos usando NewsAPI y devuelve la lista de artículos.
        """
        try:
            url = 'https://newsapi.org/v2/everything'
            params = {
                'q': f'{query}',
                'from': self.start_date.strftime('%Y-%m-%d'),
                'to': self.end_date.strftime('%Y-%m-%d'),
                'apiKey': newsapi_key,
                'sortBy': 'publishedAt',
                'pageSize': 100  # Máximo 100 artículos en el plan gratuito
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                articles = data.get('articles', [])
                logging.info(f"✅ NewsAPI query for {country_name}: {query} -> {len(articles)} articles")
                return articles
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

    def _fetch_fresh_media_articles(self, country_name: str, query: str, media_cloud_key: str = None, newsapi_key: str = None) -> list:
        """
        Fetch artículos frescos usando Media Cloud o NewsAPI.
        Implementa fallback estratégico entre fuentes.
        """
        articles = []
        
        # Primero intentar con Media Cloud si tenemos key
        if media_cloud_key:
            media_cloud_articles = self._fetch_media_cloud_articles(country_name, query, media_cloud_key)
            if media_cloud_articles is not None:
                return media_cloud_articles
            else:
                logging.warning(f"⚠️ Media Cloud failed for {country_name}, trying NewsAPI")
        
        # Fallback a NewsAPI si Media Cloud falla o no tenemos key
        if newsapi_key:
            newsapi_articles = self._fetch_newsapi_articles(country_name, query, newsapi_key)
            if newsapi_articles is not None:
                return newsapi_articles
        
        # Si ambas APIs fallan, registrar advertencia
        logging.warning(f"⚠️ Both Media Cloud and NewsAPI failed for {country_name}, returning empty list")
        return []

    def _load_existing_data(self):
        """Carga el JSON existente si lo hay."""
        if os.path.exists(self.OUTPUT_FILE):
            try:
                with open(self.OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                logging.warning("⚠️ No se pudo leer el archivo de datos existente. Se creará uno nuevo.")
        return {"metadata": {}, "results": {}}

    def _clean_old_data(self, data: dict):
        """Elimina datos de más de 30 días para no sobrecargar el archivo."""
        oldest_date = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
        for country_code in data:
            if 'daily_data' in data[country_code]:
                data[country_code]['daily_data'] = {
                    date: entry for date, entry in data[country_code]['daily_data'].items()
                    if date >= oldest_date
                }
        return data

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
        all_data = self._load_existing_data().get('results', {})

        has_fresh_data_source = media_cloud_key or newsapi_key
        if not has_fresh_data_source:
            logging.warning("⚠️ No API keys provided, will only use historical data")

        # Rango de fechas para la petición (hace 2 días)
        end_date = datetime.utcnow() - timedelta(days=2)
        start_date = end_date
        
        from_date_str = start_date.strftime('%Y-%m-%d')
        
        for code in self.country_codes:
            name = self.country_names.get(code, code)
            logging.info(f"📊 Processing data for {name} ({code})...")

            # ✅ Solución: Inicializar la estructura de datos para el país si no existe
            if code not in all_data:
                all_data[code] = {
                    "historical": {},
                    "daily_data": {},
                    "eudaimonia_predictor": 0,
                    "data_source": ""
                }
            elif "daily_data" not in all_data[code]:
                all_data[code]["daily_data"] = {}
            
            # Histórico para contexto
            hist_cpi_score = historical_cpi.get(name, {}).get('score')
            hist_gpi_score = historical_gpi.get(name, {}).get('score')
            hist_corruption = 100 - hist_cpi_score if hist_cpi_score else None
            hist_tension = hist_gpi_score if hist_gpi_score else None
            
            # Inicializar valores frescos
            fresh_corruption_index = 0
            fresh_tension_index = 0
            
            # Obtener datos frescos del día (si hay API keys)
            if has_fresh_data_source:
                try:
                    query_terms = f"{name} OR {self.corruption_terms} OR {self.tension_terms}"
                    articles = self._fetch_fresh_media_articles(name, query_terms, media_cloud_key, newsapi_key)
                    
                    if articles:
                        # Contar los términos localmente
                        corruption_count = sum(1 for article in articles if any(term in (article.get('title', '') + article.get('description', '')).lower() for term in self.corruption_terms.split(' OR ')))
                        tension_count = sum(1 for article in articles if any(term in (article.get('title', '') + article.get('description', '')).lower() for term in self.tension_terms.split(' OR ')))
                        total_count = len(articles)
                        
                        if total_count > 0:
                            fresh_corruption_index = (corruption_count / total_count * 100)
                            fresh_tension_index = (tension_count / total_count * 100)
                except Exception as e:
                    logging.error(f"❌ Error processing fresh data for {name}: {e}")
            else:
                logging.info(f"ℹ️  Skipping fresh data for {name} (no API keys)")
            
            # Actualizar datos históricos
            all_data[code]["historical"]["corruption_index"] = hist_corruption
            all_data[code]["historical"]["tension_index"] = hist_tension

            # Guardar los datos frescos del día
            daily_entry = {
                "corruption_index": round(fresh_corruption_index, 2),
                "tension_index": round(fresh_tension_index, 2),
                "data_available": has_fresh_data_source and (fresh_corruption_index > 0 or fresh_tension_index > 0)
            }
            all_data[code]["daily_data"][from_date_str] = daily_entry

            # Calcular el predictor de Eudaimonia con la media de los últimos 30 días
            recent_data_points = [
                entry for entry in all_data[code]["daily_data"].values()
                if entry["data_available"]
            ]

            if recent_data_points:
                avg_cor = sum(p["corruption_index"] for p in recent_data_points) / len(recent_data_points)
                avg_ten = sum(p["tension_index"] for p in recent_data_points) / len(recent_data_points)
                norm_cor = avg_cor / 100
                norm_ten = avg_ten / 100
            else:
                # Fallback a datos históricos si no hay datos frescos recientes
                norm_cor = (hist_corruption / 100) if hist_corruption else 0
                norm_ten = (hist_tension / 2.5) if hist_tension else 0 # Normalizando GPI de 1-5 a 0-1

            eudaimonia_predictor = 100 - (((norm_cor + norm_ten) / 2) * 100)
            eudaimonia_predictor = round(max(0, min(100, eudaimonia_predictor)), 2)
            
            all_data[code]["eudaimonia_predictor"] = eudaimonia_predictor
            all_data[code]["data_source"] = "Media Cloud/NewsAPI (fresh) + CPI/GPI (historical)" if has_fresh_data_source else "CPI/GPI (historical only)"
        
        # Limpiar datos viejos antes de guardar
        all_data = self._clean_old_data(all_data)
        
        final_data = {
            "metadata": {
                "purpose": "Predictors for Eudaimonia with historical context and fresh data",
                "processing_date": datetime.now().isoformat(),
                "time_range_fresh": f"{self.start_date.date()}",
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
    # Obtener API keys de variables de entorno
    media_cloud_key = os.environ.get('MEDIA_CLOUD_KEY')
    newsapi_key = os.environ.get('NEWSAPI_KEY')
    
    # Nuevo: Leer la lista de países de una variable de entorno
    country_codes_str = os.environ.get('COUNTRIES_TO_PROCESS')
    if country_codes_str:
        country_list = country_codes_str.split(',')
    else:
        # Fallback si no se define la variable (para pruebas locales)
        logging.warning("⚠️ No se definió la variable COUNTRIES_TO_PROCESS. Usando la lista completa como fallback.")
        country_list = [
            'USA', 'CHN', 'IND', 'BRA', 'RUS', 'JPN', 'DEU', 'GBR', 'CAN', 'FRA',
            'ITA', 'AUS', 'MEX', 'KOR', 'SAU', 'TUR', 'EGY', 'NGA', 'PAK', 'IDN',
            'VNM', 'PHL', 'ARG', 'COL', 'POL', 'ESP', 'IRN', 'ZAF', 'UKR', 'THA',
            'VEN', 'CHL', 'PER', 'MYS', 'ROU', 'SWE', 'BEL', 'NLD', 'GRC', 'CZE',
            'PRT', 'DNK', 'FIN', 'NOR', 'SGP', 'AUT', 'CHE', 'IRL', 'NZL', 'HKG',
            'ISR', 'ARE'
        ]
    
    generator = EudaimoniaPredictorGenerator(country_list)
    result = generator.generate_indices_json(
        media_cloud_key=media_cloud_key, 
        newsapi_key=newsapi_key
    )
    
    # Resumen de procesamiento
    countries_with_fresh_data = sum(1 for code, data in result['results'].items() 
                                  if 'daily_data' in data and any(d['data_available'] for d in data['daily_data'].values()))
    
    logging.info(f"📈 Procesamiento completado:")
    logging.info(f"   - Países procesados: {len(result['results'])}")
    logging.info(f"   - Países con datos frescos: {countries_with_fresh_data}")
    logging.info(f"   - Datos frescos disponibles: {result['metadata']['fresh_data_available']}")

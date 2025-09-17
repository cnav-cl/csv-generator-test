import requests
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from bs4 import BeautifulSoup
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
    
    # Usar la API de Our World in Data para ambos indicadores históricos
    CPI_URL = "https://ourworldindata.org/grapher/data/variables/corruption-perception-index.json"
    GPI_URL = "https://ourworldindata.org/grapher/data/variables/global-peace-index.json"
    
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
        Fetch CPI histórico/más reciente usando la API de Our World in Data.
        """
        try:
            logging.info("⏳ Fetching CPI data from Our World in Data API...")
            response = requests.get(self.CPI_URL)
            response.raise_for_status() # Lanza un error si la petición falla
            data = response.json()
            
            cpi_data = {}
            # La estructura del JSON de OWID es un poco compleja, navegamos hasta los datos.
            entities = data.get('entities', {})
            variables = data.get('variables', {})
            
            # Obtener el ID de la variable principal del CPI
            variable_id = list(variables.keys())[0] if variables else None
            if not variable_id:
                logging.error("❌ Could not find CPI variable in OWID data.")
                return {}
            
            # Obtener los datos del CPI
            values = variables[variable_id].get('values', [])
            entities_ids = variables[variable_id].get('entities', [])
            
            # Crear un mapeo de ID de entidad a nombre de país
            entity_name_map = {int(k): v.get('name') for k, v in entities.items()}
            
            # Navegar por los datos para encontrar la última puntuación por país
            for i, entity_id in enumerate(entities_ids):
                country_name = entity_name_map.get(entity_id)
                if country_name:
                    latest_score = values[i]
                    cpi_data[country_name] = {'score': latest_score, 'rank': None}

            logging.info(f"✅ CPI data fetched from OWID for {len(cpi_data)} countries.")
            return cpi_data
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error fetching CPI from OWID API: {e}")
            return {}
        except Exception as e:
            logging.error(f"❌ Unexpected error processing OWID CPI data: {e}")
            return {}

    def _fetch_historical_gpi(self) -> Dict[str, Any]:
        """
        Fetch GPI histórico/más reciente usando la API de Our World in Data.
        """
        try:
            logging.info("⏳ Fetching GPI data from Our World in Data API...")
            response = requests.get(self.GPI_URL)
            response.raise_for_status()
            data = response.json()
            
            gpi_data = {}
            entities = data.get('entities', {})
            variables = data.get('variables', {})
            
            variable_id = list(variables.keys())[0] if variables else None
            if not variable_id:
                logging.error("❌ Could not find GPI variable in OWID data.")
                return {}
            
            values = variables[variable_id].get('values', [])
            entities_ids = variables[variable_id].get('entities', [])
            
            entity_name_map = {int(k): v.get('name') for k, v in entities.items()}
            
            for i, entity_id in enumerate(entities_ids):
                country_name = entity_name_map.get(entity_id)
                if country_name:
                    latest_score = values[i]
                    gpi_data[country_name] = {'score': latest_score, 'rank': None}

            logging.info(f"✅ GPI data fetched from OWID for {len(gpi_data)} countries.")
            return gpi_data
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error fetching GPI from OWID API: {e}")
            return {}
        except Exception as e:
            logging.error(f"❌ Unexpected error processing OWID GPI data: {e}")
            return {}


    def _fetch_media_articles(self, country_name: str, query: str, api_key: str, api_url: str, params: Dict[str, Any], headers: Dict[str, Any]) -> Optional[list]:
        """
        Función genérica para hacer peticiones a APIs de medios.
        """
        try:
            response = requests.get(api_url, params=params, headers=headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                articles = data.get('stories', []) if 'mediacloud' in api_url else data.get('articles', [])
                logging.info(f"✅ {api_url.split('/')[2]} query for {country_name}: {query} -> {len(articles)} articles")
                return articles
            else:
                logging.warning(f"⚠️ {api_url.split('/')[2]} API error for {country_name}: {response.status_code} - {response.text}")
                return None
        except requests.exceptions.Timeout:
            logging.error(f"❌ Timeout fetching data for {country_name} from {api_url.split('/')[2]}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Request error fetching data for {country_name} from {api_url.split('/')[2]}: {e}")
            return None
        except Exception as e:
            logging.error(f"❌ Unexpected error with {api_url.split('/')[2]} API for {country_name}: {e}")
            return None

    def _fetch_fresh_media_articles(self, country_name: str, query: str, media_cloud_key: str = None, newsapi_key: str = None) -> list:
        """
        Fetch artículos frescos usando Media Cloud o NewsAPI.
        Implementa fallback estratégico entre fuentes.
        """
        articles = []
        
        # Primero intentar con Media Cloud si tenemos key
        if media_cloud_key:
            params = {
                'q': query,
                'fq': f'publish_date:[{self.start_date.strftime("%Y-%m-%d")}T00:00:00Z TO {self.end_date.strftime("%Y-%m-%d")}T23:59:59Z] AND (tags_id_media:1 OR tags_id_media:2)'
            }
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Token {media_cloud_key}'
            }
            media_cloud_articles = self._fetch_media_articles(country_name, query, media_cloud_key, 'https://api.mediacloud.org/api/v2/stories_public', params, headers)
            if media_cloud_articles is not None:
                return media_cloud_articles
            else:
                logging.warning(f"⚠️ Media Cloud failed for {country_name}, trying NewsAPI")
        
        # Fallback a NewsAPI si Media Cloud falla o no tenemos key
        if newsapi_key:
            params = {
                'q': f'{query}',
                'from': self.start_date.strftime('%Y-%m-%d'),
                'to': self.end_date.strftime('%Y-%m-%d'),
                'apiKey': newsapi_key,
                'sortBy': 'publishedAt',
                'pageSize': 100  # Máximo 100 artículos en el plan gratuito
            }
            newsapi_articles = self._fetch_media_articles(country_name, query, newsapi_key, 'https://newsapi.org/v2/everything', params, {})
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

            # Inicializar la estructura de datos para el país si no existe
            # O utilizar la estructura existente de forma segura
            country_data = all_data.get(code, {})
            if "daily_data" not in country_data:
                country_data["daily_data"] = {}
            
            # ✅ Nueva lógica: Verificar si los datos frescos ya están disponibles para la fecha
            daily_data_entry = country_data["daily_data"].get(from_date_str)

            if daily_data_entry and daily_data_entry.get('data_available'):
                logging.info(f"✅ Data for {name} on {from_date_str} already exists. Skipping fresh data fetch.")
                
            else:
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

                # Guardar los datos frescos del día
                daily_entry = {
                    "corruption_index": round(fresh_corruption_index, 2),
                    "tension_index": round(fresh_tension_index, 2),
                    "data_available": has_fresh_data_source and (fresh_corruption_index > 0 or fresh_tension_index > 0)
                }
                country_data["daily_data"][from_date_str] = daily_entry

            # Actualizar datos históricos
            hist_cpi_score = historical_cpi.get(name, {}).get('score')
            hist_gpi_score = historical_gpi.get(name, {}).get('score')
            hist_corruption = 100 - hist_cpi_score if hist_cpi_score is not None else None
            hist_tension = hist_gpi_score if hist_gpi_score is not None else None

            # Actualizar datos históricos (estos se obtienen una vez, no en el loop diario)
            country_data["historical"]["corruption_index"] = hist_corruption
            country_data["historical"]["tension_index"] = hist_tension

            # Calcular el predictor de Eudaimonia con la media de los últimos 30 días
            recent_data_points = [
                entry for entry in country_data["daily_data"].values()
                if entry.get("data_available")
            ]

            if recent_data_points:
                avg_cor = sum(p["corruption_index"] for p in recent_data_points) / len(recent_data_points)
                avg_ten = sum(p["tension_index"] for p in recent_data_points) / len(recent_data_points)
                norm_cor = avg_cor / 100
                norm_ten = avg_ten / 100
            else:
                # Fallback a datos históricos si no hay datos frescos recientes
                norm_cor = (hist_corruption / 100) if hist_corruption is not None else 0
                norm_ten = (hist_tension / 2.5) if hist_tension is not None else 0 # Normalizando GPI de 1-5 a 0-1

            eudaimonia_predictor = 100 - (((norm_cor + norm_ten) / 2) * 100)
            eudaimonia_predictor = round(max(0, min(100, eudaimonia_predictor)), 2)
            
            country_data["eudaimonia_predictor"] = eudaimonia_predictor
            country_data["data_source"] = "Media Cloud/NewsAPI (fresh) + CPI/GPI (historical)" if has_fresh_data_source else "CPI/GPI (historical only)"
            
            # Asegurar que los datos del país se guarden en el diccionario principal
            all_data[code] = country_data

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
                                  if 'daily_data' in data and any(d.get('data_available') for d in data['daily_data'].values()))
    
    logging.info(f"📈 Procesamiento completado:")
    logging.info(f"   - Países procesados: {len(result['results'])}")
    logging.info(f"   - Países con datos frescos: {countries_with_fresh_data}")
    logging.info(f"   - Datos frescos disponibles: {result['metadata']['fresh_data_available']}")

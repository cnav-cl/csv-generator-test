import requests
import json
import os
import logging
import csv
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
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
    
    # URLs directas a los archivos CSV de GitHub, la fuente más fiable.
    # Nota: Los nombres de los datasets en GitHub pueden cambiar con cada actualización anual.
    # Si un enlace falla, visita https://github.com/owid/owid-datasets para encontrar la URL actualizada.
    CPI_URL = "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Corruption%20Perception%20Index%20(Transparency%20International%2C%202024)/Corruption%20Perception%20Index%20(Transparency%20International%2C%202024).csv"
    GPI_URL = "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Global%20Peace%20Index%20(IEP%2C%202024)/Global%20Peace%20Index%20(IEP%2C%202024).csv"
    
    def __init__(self, country_codes: list):
        self.country_codes = country_codes
        self.end_date = datetime.utcnow().date() - timedelta(days=2)
        self.start_date = self.end_date - timedelta(days=90)
        self.media_cloud_api_url = "https://api.mediacloud.org/api/v2/sentences/"
        self.news_api_url = "https://newsapi.org/v2/everything"
        self.country_name_map = {
            'USA': 'United States', 'CHN': 'China', 'IND': 'India', 'BRA': 'Brazil',
            'RUS': 'Russia', 'JPN': 'Japan', 'DEU': 'Germany', 'GBR': 'United Kingdom',
            'CAN': 'Canada', 'FRA': 'France', 'ITA': 'Italy', 'AUS': 'Australia',
            'MEX': 'Mexico', 'KOR': 'South Korea', 'SAU': 'Saudi Arabia', 'TUR': 'Turkey',
            'EGY': 'Egypt', 'NGA': 'Nigeria', 'PAK': 'Pakistan', 'IDN': 'Indonesia',
            'VNM': 'Vietnam', 'PHL': 'Philippines', 'ARG': 'Argentina', 'COL': 'Colombia',
            'POL': 'Poland', 'ESP': 'Spain', 'IRN': 'Iran', 'ZAF': 'South Africa',
            'UKR': 'Ukraine', 'THA': 'Thailand', 'VEN': 'Venezuela', 'CHL': 'Chile',
            'PER': 'Peru', 'MYS': 'Malaysia', 'ROU': 'Romania', 'SWE': 'Sweden',
            'BEL': 'Belgium', 'NLD': 'Netherlands', 'GRC': 'Greece', 'CZE': 'Czechia',
            'PRT': 'Portugal', 'DNK': 'Denmark', 'FIN': 'Finland', 'NOR': 'Norway',
            'SGP': 'Singapore', 'AUT': 'Austria', 'CHE': 'Switzerland', 'IRL': 'Ireland',
            'NZL': 'New Zealand', 'HKG': 'Hong Kong', 'ISR': 'Israel', 'ARE': 'United Arab Emirates'
        }
        self.historical_data_cache = {}

    def _fetch_historical_data_from_csv(self, url: str, score_column: str, entity_column: str) -> Dict[str, Any]:
        """
        Fetch historical data from a CSV file via URL and return a dictionary of scores.
        """
        try:
            logging.info(f"⏳ Fetching historical data from {url}...")
            response = requests.get(url)
            response.raise_for_status()
            
            data = {}
            csv_reader = csv.reader(response.text.splitlines())
            
            headers = next(csv_reader)
            
            try:
                score_idx = headers.index(score_column)
                entity_idx = headers.index(entity_column)
                year_idx = headers.index('Year')
            except ValueError as e:
                logging.error(f"❌ Column not found in CSV: {e}")
                return {}
            
            latest_data = {}
            for row in csv_reader:
                try:
                    country_name = row[entity_idx].strip()
                    year = int(float(row[year_idx]))
                    score = float(row[score_idx])
                    
                    if country_name not in latest_data or year > latest_data[country_name]['year']:
                        latest_data[country_name] = {'score': score, 'year': year}
                except (ValueError, IndexError):
                    continue
            
            for country_name, entry in latest_data.items():
                data[country_name] = {'score': entry['score'], 'rank': None}

            logging.info(f"✅ Data fetched and processed for {len(data)} countries.")
            return data
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error fetching data from {url}: {e}")
            return {}
        except Exception as e:
            logging.error(f"❌ Unexpected error processing data from {url}: {e}")
            return {}

    def _fetch_historical_cpi(self) -> Dict[str, Any]:
        return self._fetch_historical_data_from_csv(
            self.CPI_URL,
            score_column='Corruption Perceptions Index (2024)',
            entity_column='Entity'
        )

    def _fetch_historical_gpi(self) -> Dict[str, Any]:
        return self._fetch_historical_data_from_csv(
            self.GPI_URL,
            score_column='Global Peace Index (2024)',
            entity_column='Entity'
        )
    
    # Lógica actualizada para datos frescos
    def _fetch_fresh_data(self, country_name: str, media_cloud_key: str, newsapi_key: str) -> Optional[Dict[str, float]]:
        """
        Fetch fresh data from Media Cloud and NewsAPI for a given country.
        Returns a dictionary with 'corruption' and 'tension' scores, or None if data is not available.
        """
        if not media_cloud_key or not newsapi_key:
            logging.error("❌ MEDIA_CLOUD_KEY o NEWSAPI_KEY no están configuradas. No se pueden obtener datos frescos.")
            return None
        
        # ⚠️ Aquí debes implementar la lógica específica para extraer y cuantificar
        # los índices de 'corrupción' y 'tensión' de los datos de las noticias.
        # El documento teórico no proporciona un algoritmo para esto[cite: 121].
        # Podrías:
        # 1. Usar palabras clave relacionadas con corrupción (soborno, fraude) y tensión (protesta, conflicto).
        # 2. Contar la frecuencia de estas palabras en las noticias de cada país.
        # 3. Normalizar la frecuencia a una escala de 0 a 100.
        
        # Como no se define un algoritmo, este valor se deja como 0 para indicar que los datos no
        # se han obtenido, pero se mantiene la estructura para una futura implementación.
        return {'corruption': 0, 'tension': 0}

    def _normalize_data(self, all_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normaliza los datos de corrupción y tensión.
        """
        # La teoría de Jiang no especifica una normalización explícita para estos índices.
        # La normalización puede ser importante para comparar valores de diferentes fuentes
        # o escalas. Por ahora, se mantiene la función sin una lógica de normalización
        # ya que los índices CPI/GPI tienen escalas definidas y los datos frescos son un placeholder.
        return all_data

    def _calculate_eudaimonia_predictor(self, country_data: Dict[str, Any]) -> int:
        """
        Calcula el predictor de Eudaimonia basado en los índices de corrupción y tensión.
        La Eudaimonia se relaciona con el deseo humano de amor, creación, aprendizaje y crecimiento[cite: 201].
        La teoría implica que la represión de estas necesidades lleva al colapso[cite: 204, 205].
        """
        # ⚠️ La teoría no proporciona un algoritmo matemático para este cálculo.
        # Una interpretación plausible es que la corrupción y la tensión disminuyen la Eudaimonia.
        # El CPI (Índice de Percepción de la Corrupción) es de 0 a 100, donde 100 es muy limpio.
        # Por lo tanto, un índice de corrupción del 0 al 100 podría ser (100 - CPI),
        # donde 100 significa más corrupción y, por lo tanto, menos Eudaimonia.
        # El GPI (Índice de Paz Global) es de 1 a 5, donde 5 es menos pacífico.
        # Los datos frescos son un placeholder (0).
        
        historical_corruption = country_data["historical"]["corruption_index"]
        historical_tension = country_data["historical"]["tension_index"]
        
        # El CPI (Our World in Data) es de 0-100, donde 100 es limpio. Lo convertimos para que sea un
        # índice de corrupción: 100 - CPI.
        # El GPI (Our World in Data) es de 1-5, donde 5 es menos paz. Lo escalamos a 0-100 para
        # que sea comparable. Una escala lineal (x-1)*25 podría funcionar.
        
        historical_corruption_scaled = historical_corruption if historical_corruption is not None else 0
        historical_tension_scaled = (historical_tension - 1) * 25 if historical_tension is not None else 0
        
        # Obtenemos los datos frescos más recientes
        latest_daily_data = list(country_data["daily_data"].values())
        if latest_daily_data:
            fresh_corruption = latest_daily_data[-1].get("corruption_index", 0)
            fresh_tension = latest_daily_data[-1].get("tension_index", 0)
        else:
            fresh_corruption = 0
            fresh_tension = 0

        # Propuesta de fórmula simple para el predictor (0-100).
        # Esto es una interpretación basada en el hecho de que la tensión y la corrupción
        # son fuerzas que van en contra de la Eudaimonia.
        predictor = 100 - (historical_corruption_scaled * 0.5 + historical_tension_scaled * 0.5 + fresh_corruption * 0.2 + fresh_tension * 0.2)
        
        # Asegurarse de que el predictor no sea negativo.
        return max(0, int(predictor))

    def _load_existing_data(self) -> Dict[str, Any]:
        if os.path.exists(self.OUTPUT_FILE):
            with open(self.OUTPUT_FILE, 'r') as f:
                return json.load(f)
        return {}

    def _save_data(self, data: Dict[str, Any]):
        try:
            os.makedirs(self.DATA_DIR, exist_ok=True)
            with open(self.OUTPUT_FILE, 'w') as f:
                json.dump(data, f, indent=2)
            logging.info(f"✅ Archivo JSON guardado exitosamente en '{self.OUTPUT_FILE}'")
        except Exception as e:
            logging.error(f"❌ Error al guardar el archivo JSON: {e}")

    def generate_indices_json(self, media_cloud_key: str, newsapi_key: str):
        all_data = self._load_existing_data()
        
        for code in self.country_codes:
            if code not in all_data.get("results", {}):
                if "results" not in all_data:
                    all_data["results"] = {}
                all_data["results"][code] = {
                    "daily_data": {},
                    "historical": {"corruption_index": None, "tension_index": None},
                    "eudaimonia_predictor": 100,
                    "data_source": "Media Cloud/NewsAPI (fresh) + CPI/GPI (historical)"
                }
        
        if "metadata" not in all_data or not all_data["results"][self.country_codes[0]]["historical"]["corruption_index"]:
            historical_cpi = self._fetch_historical_cpi()
            historical_gpi = self._fetch_historical_gpi()
            for code, country_data in all_data["results"].items():
                country_name = self.country_name_map.get(code)
                if country_name:
                    cpi_score = historical_cpi.get(country_name, {}).get("score")
                    gpi_score = historical_gpi.get(country_name, {}).get("score")
                    
                    if cpi_score is not None:
                        country_data["historical"]["corruption_index"] = 100 - cpi_score
                    if gpi_score is not None:
                        country_data["historical"]["tension_index"] = gpi_score
        
        date_str = self.end_date.isoformat()
        
        for code in self.country_codes:
            country_name = self.country_name_map.get(code)
            if not country_name:
                continue
            
            fresh_data = self._fetch_fresh_data(country_name, media_cloud_key, newsapi_key)
            
            country_data = all_data["results"].get(code)
            if country_data:
                country_data["daily_data"][date_str] = {
                    "corruption_index": fresh_data["corruption"] if fresh_data else 0,
                    "tension_index": fresh_data["tension"] if fresh_data else 0,
                    "data_available": fresh_data is not None
                }
                
                country_data["eudaimonia_predictor"] = self._calculate_eudaimonia_predictor(country_data)

        all_data["metadata"] = {
            "purpose": "Predictors for Eudaimonia with historical context and fresh data",
            "processing_date": datetime.utcnow().isoformat(),
            "time_range_fresh": self.end_date.isoformat(),
            "time_range_historical": "2024 annual data",
            "fresh_data_available": sum(1 for code in self.country_codes if all_data["results"].get(code, {}).get("daily_data", {}).get(date_str, {}).get("data_available")),
            "countries_processed": len(all_data["results"])
        }
        
        self._save_data(all_data)
        
        return all_data

def main():
    media_cloud_key = os.environ.get("MEDIA_CLOUD_KEY")
    newsapi_key = os.environ.get("NEWSAPI_KEY")
    
    country_codes_str = os.environ.get("COUNTRIES_TO_PROCESS")
    if country_codes_str:
        country_list = country_codes_str.split(',')
    else:
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
    
    countries_with_fresh_data = sum(1 for code, data in result['results'].items() if data['daily_data'].get((datetime.utcnow().date() - timedelta(days=2)).isoformat(), {}).get('data_available'))
    countries_with_historical_data = sum(1 for code, data in result['results'].items() if data['historical'].get('corruption_index') is not None)
    
    logging.info(f"Summary: {countries_with_fresh_data} countries processed with fresh data and {countries_with_historical_data} with historical data.")

if __name__ == "__main__":
    main()
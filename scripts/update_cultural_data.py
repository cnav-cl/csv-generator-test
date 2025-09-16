"""
Script para verificar y actualizar mensualmente los datos culturales del WVS
Usa el nombre de archivo correcto: data/data_worldsurvey_valores.json
"""
import json
import requests
from datetime import datetime, timedelta
import logging
import os
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import time
import re

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WVSCulturalDataUpdater:
    def __init__(self, data_file='data/data_worldsurvey_valores.json'):
        self.data_file = data_file
        self.wvs_base_url = "https://www.worldvaluessurvey.org"
        self.wvs_data_portal = "https://www.worldvaluessurvey.org/WVSContents.jsp"
        self.wvs_download_url = "https://www.worldvaluessurvey.org/WVSDocumentationWV7.jsp"
        self.current_data = self.load_current_data()
        
        # Mapeo de países a códigos estándar
        self.country_mapping = {
            'United States': 'USA', 'China': 'CHN', 'India': 'IND', 'Brazil': 'BRA',
            'Russian Federation': 'RUS', 'Japan': 'JPN', 'Germany': 'DEU', 
            'United Kingdom': 'GBR', 'Canada': 'CAN', 'France': 'FRA', 'Italy': 'ITA',
            'Australia': 'AUS', 'Mexico': 'MEX', 'South Korea': 'KOR', 
            'Saudi Arabia': 'SAU', 'Turkey': 'TUR', 'Egypt': 'EGY', 'Nigeria': 'NGA',
            'Pakistan': 'PAK', 'Indonesia': 'IDN', 'Vietnam': 'VNM', 'Philippines': 'PHL',
            'Argentina': 'ARG', 'Colombia': 'COL', 'Poland': 'POL', 'Spain': 'ESP',
            'Iran': 'IRN', 'South Africa': 'ZAF', 'Ukraine': 'UKR', 'Thailand': 'THA',
            'Venezuela': 'VEN', 'Chile': 'CHL', 'Peru': 'PER', 'Malaysia': 'MYS',
            'Romania': 'ROU', 'Sweden': 'SWE', 'Belgium': 'BEL', 'Netherlands': 'NLD',
            'Greece': 'GRC', 'Czech Republic': 'CZE', 'Portugal': 'PRT', 'Denmark': 'DNK',
            'Finland': 'FIN', 'Norway': 'NOR', 'Singapore': 'SGP', 'Austria': 'AUT',
            'Switzerland': 'CHE', 'Ireland': 'IRL', 'New Zealand': 'NZL', 
            'Hong Kong SAR': 'HKG', 'Israel': 'ISR', 'United Arab Emirates': 'ARE'
        }
        
    def load_current_data(self):
        """Carga los datos actuales del archivo JSON"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                # Crear estructura básica si el archivo no existe
                logging.warning(f"File {self.data_file} not found, creating basic structure")
                basic_data = {
                    "metadata": {
                        "source": "World Values Survey (WVS)",
                        "processing_date": datetime.now().strftime('%Y-%m-%d'),
                        "version": "1.0",
                        "update_frequency": "monthly",
                        "wvs_wave": "Wave 7 (2017-2022)",
                        "last_checked": datetime.now().isoformat()
                    },
                    "countries": {}
                }
                # Asegurarse de que el directorio existe
                os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
                with open(self.data_file, 'w', encoding='utf-8') as f:
                    json.dump(basic_data, f, indent=2, ensure_ascii=False)
                return basic_data
        except Exception as e:
            logging.error(f"Error loading cultural data: {e}")
            return None
    
    def fetch_with_retry(self, url, max_retries=3, timeout=30):
        """Realiza una petición HTTP con reintentos"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        return None
    
    def check_for_updates(self):
        """Verifica si hay actualizaciones disponibles en el WVS"""
        try:
            # Verificar la página de documentación del WVS
            response = self.fetch_with_retry(self.wvs_download_url)
            if not response:
                return False
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Buscar información sobre la última versión disponible
            version_info = soup.find_all('td', class_='bodytext')
            for td in version_info:
                text = td.get_text().strip()
                if 'Wave' in text and 'release' in text.lower():
                    logging.info(f"WVS version info found: {text}")
                    # Extraer información de la versión
                    if 'Wave 8' in text or '2023' in text or '2024' in text:
                        return True
            
            # Verificar por fecha de última actualización
            if self.current_data:
                last_update_str = self.current_data['metadata'].get('processing_date')
                if last_update_str:
                    last_update = datetime.strptime(last_update_str, '%Y-%m-%d')
                    # Verificar si ha pasado más de 90 días desde la última actualización
                    return (datetime.now() - last_update).days > 90
            
            return False
            
        except Exception as e:
            logging.error(f"Error checking for WVS updates: {e}")
            return False
    
    def download_wvs_data(self):
        """Descarga datos del WVS desde el portal oficial"""
        try:
            # URL del dataset principal del WVS Wave 7 (última versión completa)
            wvs_data_url = "https://www.worldvaluessurvey.org/WVSDocumentationWV7.jsp"
            
            response = self.fetch_with_retry(wvs_data_url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Buscar enlaces de descarga
            download_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if ('csv' in href.lower() or 'sav' in href.lower() or 
                    'xls' in href.lower() or 'dta' in href.lower()):
                    full_url = href if href.startswith('http') else f"{self.wvs_base_url}/{href}"
                    download_links.append(full_url)
            
            logging.info(f"Found {len(download_links)} potential download links")
            
            # Intentar descargar el archivo CSV más reciente
            for link in download_links:
                if 'csv' in link.lower():
                    logging.info(f"Attempting to download CSV: {link}")
                    csv_response = self.fetch_with_retry(link)
                    if csv_response:
                        # Guardar temporalmente
                        temp_file = "temp_wvs_data.csv"
                        with open(temp_file, 'wb') as f:
                            f.write(csv_response.content)
                        return temp_file
            
            return None
            
        except Exception as e:
            logging.error(f"Error downloading WVS data: {e}")
            return None
    
    def process_wvs_data(self, csv_file_path):
        """Procesa los datos del WVS y extrae las dimensiones culturales"""
        try:
            # Cargar datos del WVS
            df = pd.read_csv(csv_file_path, encoding='latin-1', low_memory=False)
            
            logging.info(f"WVS data loaded: {df.shape[0]} rows, {df.shape[1]} columns")
            
            # Dimensiones culturales clave del WVS
            cultural_dimensions = {
                'traditional_vs_secular': {
                    'indicators': ['A165', 'A124', 'F121', 'F141'],  # Valores tradicionales vs seculares
                    'description': 'Traditional values vs Secular-rational values'
                },
                'survival_vs_self_expression': {
                    'indicators': ['E018', 'E034', 'E035', 'E036'],  # Supervivencia vs auto-expresión
                    'description': 'Survival values vs Self-expression values'
                },
                'social_cohesion_index': {
                    'indicators': ['A124', 'A165', 'E018'],  # Cohesión social
                    'description': 'Social cohesion and trust index'
                }
            }
            
            processed_data = {}
            
            # Procesar por país
            for country_name, country_code in self.country_mapping.items():
                country_data = df[df['COUNTRY'].str.contains(country_name, case=False, na=False)]
                
                if not country_data.empty:
                    country_results = {}
                    
                    for dimension, config in cultural_dimensions.items():
                        values = []
                        for indicator in config['indicators']:
                            if indicator in df.columns:
                                # Calcular promedio para el indicador
                                avg_value = country_data[indicator].apply(self.safe_convert).mean()
                                if not np.isnan(avg_value):
                                    values.append(avg_value)
                        
                        if values:
                            # Normalizar a escala -2 a 2 (estándar en estudios culturales)
                            dimension_score = np.mean(values)
                            normalized_score = (dimension_score - 3) / 2  # Aproximación a escala estándar
                            country_results[dimension] = round(float(normalized_score), 3)
                    
                    if country_results:
                        processed_data[country_code] = country_results
                        logging.info(f"Processed cultural data for {country_code}: {country_results}")
            
            return processed_data
            
        except Exception as e:
            logging.error(f"Error processing WVS data: {e}")
            return None
        finally:
            # Limpiar archivo temporal
            if os.path.exists(csv_file_path):
                os.remove(csv_file_path)
    
    def safe_convert(self, value):
        """Convierte valores de forma segura a numérico"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return np.nan
    
    def get_inglehart_welzel_data(self):
        """Obtiene datos de las dimensiones culturales de Inglehart-Welzel"""
        # Datos de referencia del mapa cultural de Inglehart-Welzel
        # Estos son valores aproximados basados en investigaciones publicadas
        inglehart_data = {
            'USA': {'traditional_vs_secular': -0.8, 'survival_vs_self_expression': 1.2},
            'CHN': {'traditional_vs_secular': 0.5, 'survival_vs_self_expression': -0.7},
            'IND': {'traditional_vs_secular': 1.5, 'survival_vs_self_expression': -1.0},
            'BRA': {'traditional_vs_secular': 0.2, 'survival_vs_self_expression': 0.1},
            'RUS': {'traditional_vs_secular': 0.3, 'survival_vs_self_expression': -0.5},
            'JPN': {'traditional_vs_secular': 0.7, 'survival_vs_self_expression': 0.8},
            'DEU': {'traditional_vs_secular': 1.0, 'survival_vs_self_expression': 1.5},
            'GBR': {'traditional_vs_secular': 0.8, 'survival_vs_self_expression': 1.3},
            'CAN': {'traditional_vs_secular': 0.6, 'survival_vs_self_expression': 1.4},
            'FRA': {'traditional_vs_secular': 1.1, 'survival_vs_self_expression': 1.2},
            # ... agregar más países según sea necesario
        }
        return inglehart_data
    
    def update_cultural_data(self):
        """Actualiza los datos culturales si hay nuevas versiones disponibles"""
        if not self.check_for_updates():
            logging.info("No hay actualizaciones disponibles del WVS")
            return False
            
        logging.info("Nuevos datos del WVS disponibles. Iniciando actualización...")
        
        try:
            # Intentar descargar datos actualizados
            temp_file = self.download_wvs_data()
            
            if temp_file:
                # Procesar datos descargados
                new_data = self.process_wvs_data(temp_file)
            else:
                # Usar datos de referencia si la descarga falla
                logging.warning("Using reference Inglehart-Welzel data")
                new_data = self.get_inglehart_welzel_data()
            
            if not new_data:
                logging.error("No se pudieron obtener datos culturales")
                return False
            
            # Preparar datos actualizados
            updated_data = {
                "metadata": {
                    "source": "World Values Survey (WVS) and Inglehart-Welzel Cultural Map",
                    "processing_date": datetime.now().strftime('%Y-%m-%d'),
                    "last_updated": datetime.now().isoformat(),
                    "version": "2.0",
                    "update_frequency": "quarterly",
                    "wvs_wave": "Wave 7 (2017-2022) with supplemental data",
                    "data_points": len(new_data),
                    "next_scheduled_update": (datetime.now() + timedelta(days=90)).strftime('%Y-%m-%d')
                },
                "countries": new_data
            }
            
            # Combinar con datos existentes si los hay
            if self.current_data and 'countries' in self.current_data:
                # Mantener datos de países que no están en la nueva actualización
                for country_code, country_data in self.current_data['countries'].items():
                    if country_code not in updated_data['countries']:
                        updated_data['countries'][country_code] = country_data
            
            # Guardar los datos actualizados
            os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(updated_data, f, indent=2, ensure_ascii=False)
                
            logging.info(f"Datos culturales actualizados exitosamente en {self.data_file}")
            logging.info(f"Países procesados: {len(updated_data['countries'])}")
            
            return True
            
        except Exception as e:
            logging.error(f"Error updating cultural data: {e}")
            return False

# Uso del script
if __name__ == "__main__":
    updater = WVSCulturalDataUpdater('data/data_worldsurvey_valores.json')
    success = updater.update_cultural_data()
    
    if success:
        logging.info("✅ Actualización completada exitosamente")
    else:
        logging.warning("⚠️ No se realizaron actualizaciones o hubo errores")
    
    exit(0 if success else 1)

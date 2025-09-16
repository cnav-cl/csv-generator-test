import pandas as pd
import requests
import os

# URL del archivo de datos de Inglehart-Welzel.
# NOTA: Debes verificar esta URL periódicamente, ya que puede cambiar.
# Aquí se usa un ejemplo de una fuente común, pero busca la oficial.
DATA_URL = "https://www.worldvaluessurvey.org/WVSContents.jsp?CMSID=WVSDimensionsAndScores"

def get_data_from_html(url):
    """
    Rastrea la URL para encontrar la tabla de datos de las coordenadas.
    Esto es una técnica de web scraping, y es sensible a cambios en el sitio web.
    """
    try:
        html = requests.get(url).content
        df_list = pd.read_html(html)
        # Buscar la tabla que contiene los datos de interés.
        # Esto es un ejemplo, la estructura real puede variar.
        for df in df_list:
            if 'Country' in df.columns and 'Secular-Rational' in df.columns and 'Self-Expression' in df.columns:
                return df
    except Exception as e:
        print(f"Error al obtener datos: {e}")
        return None

def process_and_save_data():
    """
    Procesa los datos y los guarda en un archivo JSON.
    """
    # Usaremos una fuente más estable y directa, como un CSV, si está disponible.
    # Como no existe una URL pública y estable para el CSV, simularemos una.
    # En la práctica, esto podría ser una descarga manual o una API.
    # Por ahora, simularemos la obtención de datos de una fuente confiable.
    
    # Supongamos que esta es la URL del CSV con los datos.
    # Por favor, busca la URL real en el sitio de World Values Survey.
    csv_url = "https://www.worldvaluessurvey.org/some_hypothetical_link/cultural_map_data.csv"
    
    # Si la URL real no está disponible, la alternativa es hacer web scraping como en la función get_data_from_html
    # o descargar el archivo manualmente y subirlo al repositorio.
    
    try:
        # Descargamos el archivo CSV y lo leemos con pandas
        df = pd.read_csv(csv_url)
        
        # Renombramos las columnas para que sean más legibles y consistentes
        df.rename(columns={
            'Country': 'country',
            'Secular-Rational Score': 'secular_rational_score',
            'Self-Expression Score': 'self_expression_score'
        }, inplace=True)
        
        # Seleccionamos solo las columnas que nos interesan
        df = df[['country', 'secular_rational_score', 'self_expression_score']]
        
        # Guardamos el DataFrame en un archivo JSON para que sea fácil de consumir.
        output_dir = 'data'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        output_path = os.path.join(output_dir, 'cultural_data.json')
        df.to_json(output_path, orient='records', indent=4)
        print(f"Datos guardados exitosamente en {output_path}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error de red al intentar descargar los datos: {e}")
        print("El script no pudo continuar. Revisa la URL y tu conexión.")
        
    except FileNotFoundError:
        print("El archivo CSV no se encontró. Revisa la URL y el nombre del archivo.")
        
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

if __name__ == "__main__":
    process_and_save_data()

import pandas as pd
import numpy as np
from datetime import datetime
import requests

def fetch_latest_data():
    """
    Función para obtener los datos más recientes
    (Aquí debes implementar tu lógica específica para obtener los datos)
    """
    # Este es un ejemplo - debes adaptarlo a tu fuente de datos real
    try:
        # Ejemplo: obtener datos de una API o base de datos
        # response = requests.get('TU_API_URL')
        # data = response.json()
        
        # Por ahora, creamos datos de ejemplo
        current_year = datetime.now().year
        sample_data = {
            'country_code': ['USA', 'CHN', 'IND', 'BRA', 'MEX'],
            'year': [current_year, current_year, current_year, current_year, current_year],
            'estabilidad_jiang': [18.5, 15.2, 14.8, 27.1, 13.2],
            'inestabilidad_turchin': [0.0, 0.0, 0.0, 0.0, 0.0]
        }
        
        return pd.DataFrame(sample_data)
    
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def update_csv():
    # Leer el CSV existente
    try:
        existing_df = pd.read_csv('data/combined_analysis_results.csv')
    except FileNotFoundError:
        # Si el archivo no existe, crear uno vacío
        existing_df = pd.DataFrame(columns=['country_code', 'year', 'estabilidad_jiang', 'inestabilidad_turchin'])
    
    # Obtener datos nuevos
    new_data = fetch_latest_data()
    
    if new_data.empty:
        print("No new data to add")
        return
    
    # Combinar datos existentes con nuevos
    # Eliminar entradas duplicadas (mismo país y año)
    combined_df = pd.concat([existing_df, new_data])
    combined_df = combined_df.drop_duplicates(subset=['country_code', 'year'], keep='last')
    
    # Ordenar por código de país y año
    combined_df = combined_df.sort_values(by=['country_code', 'year'])
    
    # Guardar el CSV actualizado
    combined_df.to_csv('data/combined_analysis_results.csv', index=False)
    print(f"CSV updated successfully. Total rows: {len(combined_df)}")

if __name__ == "__main__":
    update_csv()

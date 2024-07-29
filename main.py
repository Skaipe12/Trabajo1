import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor

# Token de acceso a Mapbox
ACCESS_TOKEN = 'pk.eyJ1Ijoic2thaXBlMTIiLCJhIjoiY2x6NG1mc3UzM3M5bTJxcHMzYjY3OTNmbyJ9.ibafE9lySvin491TdmbkhA'

# Extraer coordenadas de los puntos u y v
def extract_coordinates(linestring):
    points = linestring.replace('LINESTRING (', '').replace(')', '').split(', ')
    u_coords = tuple(map(float, points[0].split()))
    v_coords = tuple(map(float, points[-1].split()))
    return u_coords, v_coords

# Llamar a la API de Mapbox para obtener distancia y tiempo
def get_traffic_info(row, modality):
    u_coords = row['u_coords']
    v_coords = row['v_coords']
    url = f"https://api.mapbox.com/directions/v5/mapbox/{modality}/{u_coords[0]},{u_coords[1]};{v_coords[0]},{v_coords[1]}"
    params = {
        'access_token': ACCESS_TOKEN,
        'geometries': 'geojson',
        'overview': 'simplified',
        'annotations': 'duration,speed'
    }
    response = requests.get(url, params=params)
    data = response.json()
    duration = data['routes'][0]['duration']  # en segundos
    speed = data['routes'][0]['legs'][0]['annotation']['speed'][0]  # en metros por segundo
    return row['index'], duration, speed

# Procesar los datos
def process_data(data, modality):
    data['u_coords'], data['v_coords'] = zip(*data['geometry'].apply(extract_coordinates))
    data = data.reset_index()
    durations = [None] * len(data)
    speeds = [None] * len(data)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_traffic_info, row, modality) for _, row in data.iterrows()]
        for future in futures:
            index, duration, speed = future.result()
            durations[index] = duration
            speeds[index] = speed

    data['duration_s'] = durations
    data['speed_mps'] = speeds
    data['time_car'] = data['duration_s'] / 60  # Convertir segundos a minutos
    return data

# Cargar datos
data = pd.read_csv('gdf_edges.csv')

# Ejecutar procesamiento
final_df = process_data(data[0:100], 'driving')

print(final_df.head())

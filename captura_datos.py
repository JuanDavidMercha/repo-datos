# captura_datos.py
import pandas as pd
import requests
import pytz
from datetime import datetime
import os

SEPARADOR = ';'
DEDUP_KEYS = ['Fecha', 'Hora', 'stationId']

def obtener_datos(url):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
    df = pd.read_json(data['TabularList'])
    df = df[['stationId', 'name', 'monitors']]

    # columnas dinámicas a partir de "monitors"
    all_keys = set()
    for lst in df['monitors']:
        for m in lst:
            all_keys.add(m['Name'])
    expanded = {k: [] for k in all_keys}
    expanded['stationId'] = df['stationId']
    expanded['name'] = df['name']

    for lst in df['monitors']:
        row = {k: float('nan') for k in all_keys}
        for m in lst:
            row[m['Name']] = m.get('value')
        for k in all_keys:
            expanded[k].append(row[k])

    df2 = pd.DataFrame(expanded)
    # renombres si quieres estandarizar
    df2.rename(columns={'PM25': 'PM25', 'PM10': 'PM10', 'OZONO': 'OZONO'}, inplace=True)

    tz = pytz.timezone('America/Bogota')
    now = datetime.now(tz)
    df2['Fecha'] = now.strftime('%Y-%m-%d')
    df2['Hora']  = now.strftime('%H:%M')

    cols = ['Fecha', 'Hora', 'name', 'stationId'] + [c for c in df2.columns if c not in ['Fecha','Hora','name','stationId']]
    return df2[cols]

def escribir_csv(df_nuevo, ruta_csv):
    os.makedirs(os.path.dirname(ruta_csv), exist_ok=True)
    if os.path.exists(ruta_csv):
        df_old = pd.read_csv(ruta_csv, sep=SEPARADOR)
        df = pd.concat([df_old, df_nuevo], ignore_index=True)
        # deduplicar por Fecha/Hora/stationId manteniendo la última
        keep_keys = [k for k in DEDUP_KEYS if k in df.columns]
        if keep_keys:
            df.drop_duplicates(subset=keep_keys, keep='last', inplace=True)
    else:
        df = df_nuevo
    df.to_csv(ruta_csv, sep=SEPARADOR, index=False)
    print(f"Guardado: {ruta_csv} (filas: {len(df)})")

def main():
    urls = [
        ("http://rmcab.ambientebogota.gov.co/dynamicTabulars/TabularReportTable?id=58", "Datos_Meteorologicos"),
        ("http://rmcab.ambientebogota.gov.co/dynamicTabulars/TabularReportTable?id=12", "Datos_Aire"),
    ]
    tz = pytz.timezone('America/Bogota')
    fecha = datetime.now(tz).strftime('%Y-%m-%d')

    out_aire  = f"salida/Datos_Aire_{fecha}.csv"
    out_meteo = f"salida/Datos_Meteorologicos_{fecha}.csv"

    for url, nombre in urls:
        df = obtener_datos(url)
        if nombre == "Datos_Aire":
            escribir_csv(df, out_aire)
        else:
            escribir_csv(df, out_meteo)

if __name__ == "__main__":
    main()

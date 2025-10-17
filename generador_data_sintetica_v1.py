def generar_datos_sinteticos(directorio_base, umbral_km=30, barra_progreso=None, etiqueta_estado=None):
    import os
    import pandas as pd
    import numpy as np
    from math import radians, sin, cos, sqrt, atan2

    def haversine(lat1, lon1, lat2, lon2):
        R = 6371
        phi1, phi2 = radians(lat1), radians(lat2)
        d_phi = radians(lat2 - lat1)
        d_lambda = radians(lon2 - lon1)
        a = sin(d_phi / 2)**2 + cos(phi1) * cos(phi2) * sin(d_lambda / 2)**2
        return R * 2 * atan2(sqrt(a), sqrt(1 - a))

    ruta_entrada = os.path.normpath(os.path.join(directorio_base, 'limpio'))
    ruta_salida = os.path.normpath(os.path.join(ruta_entrada, 'completado'))
    os.makedirs(ruta_salida, exist_ok=True)

    archivos = [f for f in os.listdir(ruta_entrada) if f.endswith('.csv')]
    if not archivos:
        print("[ERROR] No se encontraron archivos CSV en:", ruta_entrada)
        return

    estaciones_data = {}
    estaciones_meta = []

    for archivo in archivos:
        df = pd.read_csv(os.path.join(ruta_entrada, archivo), parse_dates=['FECHA'])
        df['FECHA'] = pd.to_datetime(df['FECHA']).dt.date
        est = df['STATION'].iloc[0] if 'STATION' in df.columns else df['STATION'].iloc[0]
        lat, lon = df['LATITUDE'].iloc[0], df['LONGITUD'].iloc[0]
        estaciones_data[est] = df
        estaciones_meta.append({'STATION': est, 'LATITUDE': lat, 'LONGITUDE': lon})

    stations_meta = pd.DataFrame(estaciones_meta)

    for i, (est_actual, df) in enumerate(estaciones_data.items()):
        if etiqueta_estado:
            etiqueta_estado.config(text=f"Procesando estación: {est_actual}")
            etiqueta_estado.update()

        lat1, lon1 = df['LATITUDE'].iloc[0], df['LONGITUD'].iloc[0]
        df['FECHA'] = pd.to_datetime(df['FECHA']).dt.date

        stations_meta['DISTANCIA'] = stations_meta.apply(
            lambda row: haversine(lat1, lon1, row['LATITUDE'], row['LONGITUDE']), axis=1
        )

        vecinos = stations_meta[
            (stations_meta['STATION'] != est_actual) &
            (stations_meta['DISTANCIA'] <= umbral_km)
        ]['STATION'].tolist()

        if not vecinos:
            print(f"[{est_actual}] No se encontraron vecinos dentro de {umbral_km} km.")
            output_path = os.path.join(ruta_salida, f"{est_actual}_completado.csv")
            df.to_csv(output_path, index=False, encoding='utf-8')
            if barra_progreso:
                barra_progreso['value'] = int((i + 1) / len(estaciones_data) * 100)
                barra_progreso.update()
            continue

        num_corregidos = {col: 0 for col in ['PRECIP', 'EVAP', 'TMAX', 'TMIN']}

        for col in num_corregidos:
            if col not in df.columns:
                continue
            df[f"{col}_FUENTE"] = np.where(df[col].isnull(), 'sin_dato', 'original')

            for idx in df[df[col].isnull()].index:
                fecha = df.loc[idx, 'FECHA']
                mes, año = fecha.month, fecha.year
                valor_usado, fuente = None, 'sin_dato'

                for vecino in vecinos:
                    vecino_df = estaciones_data[vecino]
                    resultado = vecino_df[vecino_df['FECHA'] == fecha][col].dropna()
                    if not resultado.empty:
                        valor_usado = resultado.mean()
                        fuente = 'exacto'
                        break

                if valor_usado is None:
                    for vecino in vecinos:
                        vecino_df = estaciones_data[vecino].copy()
                        vecino_df['FECHA'] = pd.to_datetime(vecino_df['FECHA'])
                        vecino_df['YEAR'] = vecino_df['FECHA'].dt.year
                        vecino_df['MONTH'] = vecino_df['FECHA'].dt.month
                        media = vecino_df[(vecino_df['YEAR'] == año) & (vecino_df['MONTH'] == mes)][col].dropna().mean()
                        if not pd.isna(media):
                            valor_usado = media
                            fuente = 'promedio_mes_año'
                            break

                if valor_usado is None:
                    for vecino in vecinos:
                        vecino_df = estaciones_data[vecino].copy()
                        vecino_df['FECHA'] = pd.to_datetime(vecino_df['FECHA'])
                        vecino_df['MONTH'] = vecino_df['FECHA'].dt.month
                        media = vecino_df[vecino_df['MONTH'] == mes][col].dropna().mean()
                        if not pd.isna(media):
                            valor_usado = media
                            fuente = 'promedio_mes'
                            break

                if valor_usado is not None:
                    df.at[idx, col] = valor_usado
                    df.at[idx, f"{col}_FUENTE"] = fuente
                    num_corregidos[col] += 1

        output_path = os.path.join(ruta_salida, f"{est_actual}_completado.csv")
        df.to_csv(output_path, index=False, encoding='utf-8')
        resumen = ', '.join([f"{k}: {v}" for k, v in num_corregidos.items()])
        print(f"[{est_actual}] Correcciones realizadas → {resumen}")

        if barra_progreso:
            barra_progreso['value'] = int((i + 1) / len(estaciones_data) * 100)
            barra_progreso.update()

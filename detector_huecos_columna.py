import pandas as pd
import os

def detectar_huecos_columnas(df: pd.DataFrame, columnas: list, fecha_col: str = 'FECHA', archivo_csv_original: str = None):
    """
    Detecta secuencias de valores nulos (huecos) en las columnas especificadas
    y calcula la duración de cada hueco.

    Args:
        df: El DataFrame de entrada.
        columnas: Una lista de nombres de columnas a analizar.
        fecha_col: El nombre de la columna que contiene las fechas (debe ser datetime).
        archivo_csv_original (str): Ruta del archivo CSV original (ej. .../limpio/NR_1234.csv).

    Returns:
        Un DataFrame con los resultados de los huecos (inicio, fin, duración, tipo).
    """
    
    df_temp = df.copy()
    df_temp = df_temp.sort_values(fecha_col)
    
    resultados = []

    for col in columnas:
        # Se analiza cada columna por separado
        is_null = df_temp[col].isnull()
        # Identificar inicios de huecos comparando con el valor anterior
        es_inicio = is_null & (~is_null.shift(1, fill_value=False))
        # Crear un ID de grupo para cada secuencia de nulos utilizando la suma acumulada de inicios
        group_id = es_inicio.cumsum()
        
        df_huecos = df_temp[is_null].copy()
        df_huecos['group_id'] = group_id[is_null]
        
        if df_huecos.empty:
            continue

        agg_results = df_huecos.groupby('group_id').agg(
            inicio=(fecha_col, 'min'),
            fin=(fecha_col, 'max')
        )
        
        # Calcular la duración de los huecos
        agg_results['duracion'] = (agg_results['fin'] - agg_results['inicio']).dt.days + 1
        
        tipo_huecos = []
        for duracion in agg_results['duracion']:
            if duracion <= 3:
                tipo_huecos.append('corto')
            else:
                tipo_huecos.append('largo')
        agg_results['tipo'] = tipo_huecos
        
        agg_results['columna'] = col
        
        resultados.append(agg_results[['columna', 'inicio', 'fin', 'duracion', 'tipo']])
    
    # Guardar resultados en un csv 
    if resultados:
        df_resultados = pd.concat(resultados, ignore_index=True)
    else:
        # Si no hay huecos, el DF de resultados está vacío
        df_resultados = pd.DataFrame(columns=['columna', 'inicio', 'fin', 'duracion', 'tipo'])
        
    if archivo_csv_original:
        # Lógica de rutas (Retrocede a directorio_base y crea 'huecos_columnas')
        directorio_base = os.path.dirname(os.path.dirname(archivo_csv_original))
        carpeta_salida = os.path.join(directorio_base, 'huecos_columnas')
        os.makedirs(carpeta_salida, exist_ok=True)
        
        base_name, ext = os.path.splitext(os.path.basename(archivo_csv_original))
        nombre_archivo_hueco = f'{base_name}_HuecosColumnas{ext}'
        archivo_salida_final = os.path.join(carpeta_salida, nombre_archivo_hueco)
        
        df_resultados.to_csv(archivo_salida_final, index=False)
        
        columnas_afectadas = df_resultados['columna'].unique().tolist() if not df_resultados.empty else 'Ninguna'
        
        print(f"Resultados de huecos de columna guardados en: {archivo_salida_final}")
        print(f"Columnas/atributos con huecos detectados: {', '.join(columnas_afectadas)}")
            
    return df_resultados
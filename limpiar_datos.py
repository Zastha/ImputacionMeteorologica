import pandas as pd
import re
import os
import detector_huecos_columna as dhc
import imputacion_columnas as ic
from numpy import nan

def procesar_lote_txt_a_csv(directorio_entrada, umbral_faltantes=100, generar_logs=False):
    """procesar_lote_txt_a_csv(directorio_entrada):
    
    This function receives a directory, detects the .txt files in it and processes them using the limpiar_txt_a_csv function.

    Input:
        directorio_entrada (String): The directory where the .txt files are located.
        umbral_faltantes (int): Maximum percentage of missing dates allowed.
        generar_logs (bool): If True, generates a CSV file for quality data logging purposes.
    Return:
        None
    """
    
    if generar_logs:
        crear_archivo_log_calidad(directorio_entrada)
        
    directorio_salida = os.path.join(directorio_entrada, "limpio")    
    os.makedirs(directorio_salida, exist_ok=True)
    for archivo in os.listdir(directorio_entrada):
        if archivo.endswith(".txt"):
            archivo_txt = os.path.join(directorio_entrada, archivo)
            if generar_logs:
                log_actual = os.path.join(directorio_entrada, f'log_calidad.csv')
              
                limpiar_txt_a_csv(archivo_txt, directorio_entrada, generar_logs)
                log_actual = os.path.join(directorio_entrada, f'log_calidad.csv')
                rellenar_fechas_faltantes(os.path.join(directorio_salida, f'NR_{archivo.replace(".txt", "")}.csv'), umbral_faltantes, log_actual, generar_logs)
            else:
                limpiar_txt_a_csv(archivo_txt, directorio_entrada)
                rellenar_fechas_faltantes(os.path.join(directorio_salida, f'NR_{archivo.replace(".txt", "")}.csv'), umbral_faltantes)

def crear_archivo_log_calidad(directorio_entrada):
    """crear_archivo_log_calidad(directorio_entrada):

    This function creates a CSV file for quality data logging purposes.
    
    Input:
        directorio_entrada (String): The directory where the log file will be created.
    Return:
        None
    """
    cols_logs = ['STATION', 'NOMBRE', 'NUM_FECHAS_PRELLENADO',
                 'NUM_FECHAS_POSLLENADO', 'NUM_FECHAS_FALTANTES','PORCENTAJE_FECHAS_FALTANTES',
                 'PRECIP_NULOS','PORCENTAJE_PRECIP_NULO','NUM_PRECIP_CORREGIDOS',
                 'PORCENTAJE_PRECIP_CORREGIDOS',  'EVAP_NULOS', 'PORCENTAJE_EVAP_NULOS',
                 'NUM_EVAP_CORREGIDOS', 'PORCENTAJE_EVAP_CORREGIDOS', 'TMAX_NULOS',
                 'PORCENTAJE_TMAX_NULOS','NUM_TMAX_CORREGIDOS','PORCENTAJE_TMAX_CORREGIDOS',
                 'TMIN_NULOS','PORCENTAJE_TMIN_NULOS', 'NUM_TMIN_CORREGIDOS',
                 'PORCENTAJE_TMIN_CORREGIDOS']
    
    df_log_calidad = pd.DataFrame(columns=cols_logs)
    archivo_salida_log = os.path.join(directorio_entrada, f'log_calidad.csv')
    df_log_calidad.to_csv(archivo_salida_log, index=False, encoding='utf-8')
    print(f'Archivo de log de calidad generado en: {directorio_entrada}')

def limpiar_txt_a_csv(archivo_txt, directorio_entrada, generar_logs=False):
    """limpiar_txt_a_csv(archivo_txt, directorio_salida):

    This function receives a .txt file with a specific structure, then it cleans and processes it and saves it as a .csv file.
    Input:
       - archivo_txt (File): A .txt file with a specific structure.
       - directorio_salida (String): The directory where the .csv file will be saved.
       - generar_logs (bool): If True, fills some of the log quality data (station_id, name).
    Return: 
        None
    """
    directorio_salida = os.path.join(directorio_entrada, "limpio")
    if not os.path.exists(directorio_salida):
        os.makedirs(directorio_salida)
        
    codificacion = "utf-8"
    # REGEX para detectar la fecha en el formato YYYY-MM-DD
    regex = r'\d{4}-\d{2}-\d{2}'
    # Después de la línea 25 termina la cabecera, se espera que los datos comiencen
    linea_inicio = 25
    
    with open(archivo_txt, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f'Procesando archivo {archivo_txt}')
    print(f'Número de líneas: {len(lineas)}')
    
    target_lines = [10, 11, 12, 13, 14, 16, 17, 18]
    etiquetas = []
    valores = []
    estacion = "Unknown"
    
    for i in target_lines:
        if i < len(lineas) and ':' in lineas[i]:
            clave, valor = map(str.strip, re.split(r':\s*', lineas[i], maxsplit=1))
            valor = valor.replace('�', '').replace('°', '').replace('ｰ', '').replace("ï¿½", "")
            if clave == 'ALTITUD':
                valor = re.sub(r'\s*msnm', '', valor)
            etiquetas.append(clave)
            valores.append(valor)
        
        if "ESTACIÓN" in lineas[i]:  # Detección explícita de la estación
            match = re.search(r'ESTACIÓN\s*:\s*(\d+)', lineas[i])
            if match:
                estacion = match.group(1)
        if "ESTACION" in lineas[i]:
            match = re.search(r'ESTACION\s*:\s*(\d+)', lineas[i])
            if match:
                estacion = match.group(1)
    
    print(f'Estacion detectada: {estacion}')
    datos = []
    
    for linea in lineas[linea_inicio:]:
        if re.match(regex, linea):
            valores_linea = re.split(r'\t+', linea.strip()) if '\t' in linea else re.split(r'\s+', linea.strip())
            if len(valores_linea) >= 5:
                datos.append(valores_linea[:5])
    
    if not datos:
        print("⚠️ Advertencia: No se encontraron datos climáticos en el archivo.")
        return
    
    columnas = ['FECHA', 'PRECIP', 'EVAP', 'TMAX', 'TMIN'] + etiquetas
    df = pd.DataFrame(datos, columns=columnas[:len(datos[0])])
    
    
    for i, etiqueta in enumerate(etiquetas):
        df[etiqueta] = valores[i] if i < len(valores) else ""
    
    df.replace('Nulo', None, inplace=True)
    for column in ['PRECIP', 'EVAP', 'TMAX', 'TMIN']:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    
    df['FECHA'] = pd.to_datetime(df['FECHA'], format='%Y-%m-%d')
    df = df.dropna(subset=['FECHA'])
    df['MONTH'] = df['FECHA'].dt.month
    df['YEAR'] = df['FECHA'].dt.year
    
    
    ## Rellenar valores nulos por el promedio del mes
    #for column in ['PRECIP', 'EVAP', 'TMAX', 'TMIN']:
    #    df[column] = df.groupby(['YEAR', 'MONTH'])[column].transform(lambda x: x.fillna(x.mean() if not pd.isna(x.mean()) else nan))
    
    #for column in ['PRECIP', 'EVAP', 'TMAX', 'TMIN']:
    #    df[column] = df[column].astype(str).replace(nan, 'null')

    archivo_salida = os.path.join(directorio_salida, f'NR_{estacion}.csv')
    df.to_csv(archivo_salida, index=False, encoding='utf-8')
    print(f'Archivo CSV guardado como {archivo_salida}')
    
    if generar_logs:
        log_data = {
            'STATION': estacion,
            'NOMBRE': valores[1] if len(valores) > 1 else "Desconocida",
        }
        agg_datos_estacion_log_calidad(log_data, directorio_entrada)

            
def agg_datos_estacion_log_calidad(log_data,  directorio_entrada):
    """agg_datos_estacion_log_calidad(log_data, directorio_entrada):
    This function aggregates the log data for a specific station and updates the quality log file.
    Input:
        log_data (dict): Dictionary containing the log data for the station.
        directorio_entrada (String): Directory where the log file is located.
    Return:
        None
    """  
    archivo_log_calidad = os.path.join(directorio_entrada, 'log_calidad.csv')
    df_log_calidad = pd.read_csv(archivo_log_calidad, encoding='utf-8')
    df_log_calidad.loc[len(df_log_calidad)] = log_data
    ##  Sobreescribe el archivo de log de calidad
    directorio_salida_log = os.path.join(directorio_entrada, 'log_calidad.csv')
    df_log_calidad.to_csv(directorio_salida_log, index=False, encoding='utf-8')
    print(f'Log de calidad actualizado en: {directorio_entrada}')

def rellenar_fechas_faltantes(archivo_csv, umbral_faltantes=100, archivo_log = nan, generar_logs_calidad=False):
    
    """rellenar_fechas_faltantes(archivo_csv, umbral_faltantes=100, generar_logs_calidad=False):

    This function receives the processed csv file and fills in the missing dates by calculating the min-max date range.
        
    Input:
       - archivo_csv (File): csv file with missing dates.
       - umbral_faltantes (int): Maximum percentage of missing dates allowed.
       - generar_logs_calidad (bool): If True, generates logs for missing dates. 
    Return: 
        None
    """
    
    df = pd.read_csv(archivo_csv, dayfirst=True)
    df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
    
    # Contar número de registros en el Dataframe df
    total_registros_original = df.shape[0]
    
    fecha_min = df['FECHA'].min()
    fecha_max = df['FECHA'].max()
    total_fechas = pd.date_range(start=fecha_min, end=fecha_max, freq='D')
    
    all_dates = pd.DataFrame({'FECHA': total_fechas})
    
    df_completo = all_dates.merge(df, on='FECHA', how='left')
    total_registros_generados = df_completo.shape[0]
    fechas_faltantes = total_registros_generados - total_registros_original
    ##Calculo del porcentaje de fechas faltantes
    porcentaje_faltante = (fechas_faltantes / total_registros_generados) * 100
    
    ## Si el porcentaje de fechas faltantes es mayor al umbral, se elimina el archivo y se detiene el proceso
    if porcentaje_faltante > umbral_faltantes:
        faltantes_msg = f"Entradas faltantes en {archivo_csv}\nTotal de entradas faltantes: {fechas_faltantes}\nPorcentaje de entradas faltantes: {porcentaje_faltante:.2f}%"
        print(faltantes_msg)
        print(f'El porcentaje de fechas faltantes es mayor al umbral permitido ({umbral_faltantes}%)')
        os.remove(archivo_csv)
        print(f'Archivo descartado: {archivo_csv}')
        return    
    else:
        
       
        dhc.detectar_huecos_columnas(
            df=df, 
            columnas=['PRECIP', 'EVAP', 'TMAX', 'TMIN'], 
            fecha_col='FECHA', 
            archivo_csv_original=archivo_csv 
        )
        
        
        
        csv_col_cortas_imp = ic.imputar_cortos(
            archivo_csv_base=archivo_csv)
        
        csv_col_largas_imp = ic.imputar_largos(
            archivo_csv_imputado_corto=csv_col_cortas_imp)
        
        
        
        
        
        df_completo['MONTH'] = df_completo['FECHA'].dt.month
        df_completo['YEAR'] = df_completo['FECHA'].dt.year
        
     #   if not df_completo.empty:
      #      if 'ESTACION' in df.columns:
     #           constantes = ['STATION', 'NOMBRE', 'STATUS', 'MUNICIPality', 'STATUS', 'LATITUDE', 'LONGITUDE', 'ALTITUDE']
      #      else:
      #          constantes = ['STATION', 'NOMBRE', 'STATUS', 'MUNICIPality', 'STATUS', 'LATITUDE', 'LONGITUDE', 'ALTITUDE']
      #      for const in constantes:
       #         df_completo[const] = df[const].iloc[0] if const in df.columns and not df[const].isna().all() else "Desconocida"          
           # for column in ['PRECIP', 'EVAP', 'TMAX', 'TMIN']:
           #     df_completo[column] = df_completo[column].astype(str).replace(nan, 'null')
           #     df_completo[column] = df_completo[column].astype(str).replace('nan', 'null')
        #df_completo = df_completo.astype(str).replace('nan', 'null')
        df.to_csv(archivo_csv, index=False, encoding='utf-8')    
    log_msg = f"Serie de tiempo reconstruida para {archivo_csv}:\nTotal de entradas faltantes: {fechas_faltantes}\nPorcentaje de entradas faltantes: {porcentaje_faltante:.2f}%"
    print(log_msg)
    
    if generar_logs_calidad:
        precip_nulos = df_completo['PRECIP'].isnull().sum()
        evap_nulos   = df_completo['EVAP'].isnull().sum()
        tmax_nulos   = df_completo['TMAX'].isnull().sum()
        tmin_nulos   = df_completo['TMIN'].isnull().sum()

        log_data = {
            'STATION': df_completo['STATION'].iloc[0] if 'STATION' in df_completo.columns else df_completo['STATION'].iloc[0],
            'NOMBRE': df_completo['NOMBRE'].iloc[0] if 'NOMBRE' in df_completo.columns else "UNKNOWN",
            'QT_PREFILLED_ROWS': total_registros_original,
            'QT_POST_FILLED_ROWS': total_registros_generados,
            'QT_MISSING_DATA': fechas_faltantes,
            'RATIO MISSING_DATA': porcentaje_faltante,
            'PRECIP_NULOS': precip_nulos,
            'RATIO_PRECIP_NULL': (precip_nulos / total_registros_generados) * 100,
            'EVAP_NULLS': evap_nulos,
            'RATIO_EVAP_NULLS': (evap_nulos / total_registros_generados) * 100,
            'TMAX_NULLS': tmax_nulos,
            'RATIO_TMAX_NULLS': (tmax_nulos / total_registros_generados) * 100,
            'TMIN_NULLS': tmin_nulos,
            'RATIO_TMIN_NULLS': (tmin_nulos / total_registros_generados) * 100
}

        agg_datos_nulos_log_calidad(log_data, os.path.dirname(archivo_log))
## Comment para ver si me deja mergear los cambios
def agg_datos_nulos_log_calidad(log_data, directorio_log):
    """agg_datos_nulos_log_calidad(log_data, directorio_log):
    
    This function aggregates the log data for missing values and updates the quality log file.
    
    Input:
        log_data (dict): Dictionary containing the log data for missing values.
        directorio_log (String): Directory where the log file is located.
    Return:
        None
    """
    archivo_log_calidad = os.path.join(directorio_log, 'log_calidad.csv')
    df_log_calidad = pd.read_csv(archivo_log_calidad, encoding='utf-8')
    df_log_calidad.loc[len(df_log_calidad)-1] = log_data
    ##  Sobreescribe el archivo de log de calidad
    directorio_salida_log = os.path.join(directorio_log, 'log_calidad.csv')
    df_log_calidad.to_csv(directorio_salida_log, index=False, encoding='utf-8')
    print(f'Log de calidad actualizado en: {directorio_log}')

def agg_datos_sinteticos_log_calidad(directorio_entrada):
    """Agrega al log de calidad cuántos datos nulos fueron corregidos para cada estacion."""
    import os
    import pandas as pd

    ruta_log = os.path.join(directorio_entrada, 'log_calidad.csv')
    carpeta_completado = os.path.join(directorio_entrada, 'limpio', 'completado')

    if not os.path.exists(ruta_log) or not os.path.exists(carpeta_completado):
        print("[ERROR] No se encontro log_calidad.csv o la carpeta 'limpio/completado'")
        return

    df_log = pd.read_csv(ruta_log, encoding='utf-8')
    archivos_completados = [f for f in os.listdir(carpeta_completado) if f.endswith('.csv')]

    for archivo in archivos_completados:
        try:
            estacion_id = int(archivo.split('_')[0])
        except ValueError:
            print(f"[ADVERTENCIA] No se pudo extraer estacion de: {archivo}")
            continue

        df_corregido = pd.read_csv(os.path.join(carpeta_completado, archivo))

        if estacion_id not in df_log['STATION'].astype(int).values:
            print(f"[ADVERTENCIA] Estacion {estacion_id} no está en el log.")
            continue

        idx = df_log[df_log['STATION'].astype(int) == estacion_id].index[0]

        # Obtener valores originales con manejo de "null"
        orig_precip = pd.to_numeric(df_log.at[idx, 'PRECIP_NULOS'], errors='coerce')
        orig_evap   = pd.to_numeric(df_log.at[idx, 'EVAP_NULOS'], errors='coerce')
        orig_tmax   = pd.to_numeric(df_log.at[idx, 'TMAX_NULOS'], errors='coerce')
        orig_tmin   = pd.to_numeric(df_log.at[idx, 'TMIN_NULOS'], errors='coerce')

        orig_precip = 0 if pd.isna(orig_precip) else orig_precip
        orig_evap   = 0 if pd.isna(orig_evap) else orig_evap
        orig_tmax   = 0 if pd.isna(orig_tmax) else orig_tmax
        orig_tmin   = 0 if pd.isna(orig_tmin) else orig_tmin

        # Contar nulos actuales
        nuevos_precip = df_corregido['PRECIP'].isnull().sum()
        nuevos_evap = df_corregido['EVAP'].isnull().sum()
        nuevos_tmax = df_corregido['TMAX'].isnull().sum()
        nuevos_tmin = df_corregido['TMIN'].isnull().sum()

        # Cálculos
        df_log.at[idx, 'NUM_PRECIP_CORREGIDOS'] = orig_precip - nuevos_precip
        df_log.at[idx, 'PORCENTAJE_PRECIP_CORREGIDOS'] = ((orig_precip - nuevos_precip) / orig_precip) * 100 if orig_precip > 0 else 0

        df_log.at[idx, 'NUM_EVAP_CORREGIDOS'] = orig_evap - nuevos_evap
        df_log.at[idx, 'PORCENTAJE_EVAP_CORREGIDOS'] = ((orig_evap - nuevos_evap) / orig_evap) * 100 if orig_evap > 0 else 0

        df_log.at[idx, 'NUM_TMAX_CORREGIDOS'] = orig_tmax - nuevos_tmax
        df_log.at[idx, 'PORCENTAJE_TMAX_CORREGIDOS'] = ((orig_tmax - nuevos_tmax) / orig_tmax) * 100 if orig_tmax > 0 else 0

        df_log.at[idx, 'NUM_TMIN_CORREGIDOS'] = orig_tmin - nuevos_tmin
        df_log.at[idx, 'PORCENTAJE_TMIN_CORREGIDOS'] = ((orig_tmin - nuevos_tmin) / orig_tmin) * 100 if orig_tmin > 0 else 0

    df_log.to_csv(ruta_log, index=False, encoding='utf-8')
    print("✔ Log de calidad actualizado con datos sintéticos.")


def generar_concentrados_diarios(directorio_entrada):
    """generar_concentrados_diarios(directorio_entrada):
    
    This function receives the directory of the raw files, locates the clean files folder and generates a single file containing all the daily summaries.

    Input:
        directorio_entrada (String): Folder where the RAW files are located
    Return: None
    """
    directorio_limpios = os.path.join(directorio_entrada, "limpio")
    archivos_csv = [os.path.join(directorio_limpios, f) for f in os.listdir(directorio_limpios) if f.endswith(".csv")]
    df_final = pd.concat([pd.read_csv(f) for f in archivos_csv], ignore_index=True)
    #for column in ['PRECIP', 'EVAP', 'TMAX', 'TMIN']:
    #    df_final[column] = df_final[column].astype(str).replace(nan, 'null')
    #    df_final[column] = df_final[column].astype(str).replace('nan', 'null')
    archivo_final = os.path.join(directorio_limpios, 'CD.csv')
    df_final.to_csv(archivo_final, index=False, encoding='utf-8')
    print(f'Archivo final de concentrado diario guardado como {archivo_final}')

def generar_concentrados_mensuales(directorio_entrada):
    """generar_concentrados_mensuales(directorio_entrada):
    
    This function receives the directory of the raw files, then locates the processed files folder and generates monthly summaries for each station.

    Input:
        directorio_entrada (String): folder where the original files are located
    Return:
        None
    """
    directorio_limpios = os.path.join(directorio_entrada, "limpio")
    directorio_concentrado = os.path.join(directorio_entrada, "CM")
    os.makedirs(directorio_concentrado, exist_ok=True)
    archivos_csv = [os.path.join(directorio_limpios, f) for f in os.listdir(directorio_limpios) if f.endswith(".csv")]
    
    for archivo in archivos_csv:
        df = pd.read_csv(archivo)
        
        df_mensual = df.groupby(['YEAR', 'MONTH']).agg({
            'PRECIP': 'sum',
            'EVAP': 'mean',
            'TMAX': 'max',
            'TMIN': 'min'
        }).reset_index()
        
        if not df_mensual.empty:
            if 'STATION' in df.columns:
                constantes = ['STATION', 'NOMBRE', 'ESTADO', 'MUNICIPIO', 'SITUACION', 'LATITUD', 'LONGITUD', 'ALTITUD']
            else:
                constantes = ['STATION', 'NOMBRE', 'ESTADO', 'MUNICIPIO', 'SITUACION', 'LATITUD', 'LONGITUD', 'ALTITUD']
            for const in constantes:
                df_mensual[const] = df[const].iloc[0] if const in df.columns and not df[const].isna().all() else "Desconocida"
        
        #    for column in ['PRECIP', 'EVAP', 'TMAX', 'TMIN']:
        #        df_mensual[column] = df_mensual[column].astype(str).replace(nan, 'null')
        #        df_mensual[column] = df_mensual[column].astype(str).replace('nan', 'null')

            if 'STATION' in df.columns:
                archivo_concentrado = os.path.join(directorio_concentrado, f'CM_{df["STATION"].iloc[0]}.csv')
            else:
                archivo_concentrado = os.path.join(directorio_concentrado, f'CM_{df_mensual["ESTACION"].iloc[0]}.csv')
            df_mensual.to_csv(archivo_concentrado, index=False, encoding='utf-8')
            print(f'Archivo de concentrado mensual guardado como {archivo_concentrado}')

def concatenar_concentrados_mensuales(directorio_entrada):
    """concatenar_concentrados_mensuales(directorio_entrada):
    
    This function receives the directory of the raw files, then locates the monthly summaries folder and concatenates them into a single .csv file.

    Input:
        directorio_entrada (String): folder where the raw files are located
    Return:
        None
    """
    directorio_concentrado = os.path.join(directorio_entrada, "CM")
    directorio_final = os.path.join(directorio_entrada, "FINAL")
    
    os.makedirs(directorio_final, exist_ok=True)
    
    archivos_csv = [os.path.join(directorio_concentrado, f) for f in os.listdir(directorio_concentrado) if f.endswith(".csv")]
    
    df_final = pd.concat([pd.read_csv(f) for f in archivos_csv], ignore_index=True)
    # for column in ['PRECIP', 'EVAP', 'TMAX', 'TMIN']:
    #    df_final[column] = df_final[column].astype(str).replace(nan, 'null')
    #    df_final[column] = df_final[column].astype(str).replace('nan', 'null')
    archivo_final = os.path.join(directorio_final, 'FINAL.csv')
    df_final.to_csv(archivo_final, index=False, encoding='utf-8')
    print(f'Archivo final concatenado guardado como {archivo_final}')

def generar_concentrados_anuales(directorio_entrada):
    """generar_concentrados_anuales(directorio_entrada):
    
    This function receives the directory of the raw files, then locates the processed files folder and generates annual summaries for each station.

    Input:
        directorio_entrada (String): folder where the original files are located
    Return:
        None
    """
    directorio_limpios = os.path.join(directorio_entrada, "limpio/completado")
    directorio_concentrado = os.path.join(directorio_entrada, "CA")
    os.makedirs(directorio_concentrado, exist_ok=True)
    archivos_csv = [os.path.join(directorio_limpios, f) for f in os.listdir(directorio_limpios) if f.endswith(".csv")]
    
    for archivo in archivos_csv:
        df = pd.read_csv(archivo)
        
        df_anual = df.groupby(['YEAR']).agg({
            'PRECIP': 'sum',
            'EVAP': 'mean',
            'TMAX': 'max',
            'TMIN': 'min'
        }).reset_index()
        
        if not df_anual.empty:
            if 'STATION' in df.columns:
                constantes = ['STATION', 'NOMBRE', 'ESTADO', 'MUNICIPIO', 'SITUACIÓN', 'LATITUD', 'LONGITUD', 'ALTITUD']
            else:
                constantes = ['STATION', 'NOMBRE', 'ESTADO', 'MUNICIPIO', 'SITUACIÓN', 'LATITUD', 'LONGITUD', 'ALTITUD']
            for const in constantes:
                df_anual[const] = df[const].iloc[0] if const in df.columns and not df[const].isna().all() else "Desconocida"
        
            #for column in ['PRECIP', 'EVAP', 'TMAX', 'TMIN']:
            #    df_anual[column] = df_anual[column].astype(str).replace(nan, 'null')
            #    df_anual[column] = df_anual[column].astype(str).replace('nan', 'null')

            if 'STATION' in df.columns:
                archivo_concentrado = os.path.join(directorio_concentrado, f'CA_{df["STATION"].iloc[0]}.csv')
            else:
                archivo_concentrado = os.path.join(directorio_concentrado, f'CA_{df_anual["ESTACIÓN"].iloc[0]}.csv')
            df_anual.to_csv(archivo_concentrado, index=False, encoding='utf-8')
            print(f'Archivo de concentrado anual guardado como {archivo_concentrado}')
            
            
def concatenar_concentrados_anuales(directorio_entrada):
    """concatenar_concentrados_anuales(directorio_entrada):
    
    This function receives the directory of the raw files, then locates the annual summaries folder and concatenates them into a single .csv file.

    Input:
        directorio_entrada (String): folder where the raw files are located
    Return:
        None
    """
    directorio_concentrado = os.path.join(directorio_entrada, "CA")
    directorio_final = os.path.join(directorio_entrada, "FINAL")
    
    os.makedirs(directorio_final, exist_ok=True)
    
    archivos_csv = [os.path.join(directorio_concentrado, f) for f in os.listdir(directorio_concentrado) if f.endswith(".csv")]
    
    df_final = pd.concat([pd.read_csv(f) for f in archivos_csv], ignore_index=True)
    #for column in ['PRECIP', 'EVAP', 'TMAX', 'TMIN']:
    #    df_final[column] = df_final[column].astype(str).replace(nan, 'null')
    #    df_final[column] = df_final[column].astype(str).replace('nan', 'null')
    archivo_final = os.path.join(directorio_final, 'FINAL_ANUAL.csv')
    df_final.to_csv(archivo_final, index=False, encoding='utf-8')
    print(f'Archivo final anual concatenado guardado como {archivo_final}')
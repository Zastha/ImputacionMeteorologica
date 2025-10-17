import os
import pandas as pd
import RFColLargos as rfImputacion

def imputar_cortos(archivo_csv_base: str):
    """
    Aplica interpolación lineal a los huecos de datos de duración corta (<= 3 días)
    en el archivo base, utilizando el reporte de huecos. Crea un nuevo archivo CSV 
    con las correcciones en una subcarpeta llamada 'imputacion-columnas-cortas'.

    Args:
        archivo_csv_base (str): Ruta del archivo CSV principal con los datos 
                                completados con fechas (ej. .../limpio/NR_1234.csv).
    
    Returns:
        str: La ruta donde se guardó el archivo corregido.
    """
    
    # Define la ruta del archivo de salida a partir del archivo base
    dir_base = os.path.dirname(os.path.dirname(archivo_csv_base))
    carpeta_salida = os.path.join(dir_base, 'imputacion-columnas-cortas')
    os.makedirs(carpeta_salida, exist_ok=True)
    
    base_name, ext = os.path.splitext(os.path.basename(archivo_csv_base))
    reporte_huecos_path = os.path.join(dir_base, 'huecos_columnas', f'{base_name}_HuecosColumnas{ext}')
    
    if not os.path.exists(reporte_huecos_path):
        print(f"Error: No se encontró el reporte de huecos en {reporte_huecos_path}")
        return None
        
    #Define dataframe principal 
    df_principal = pd.read_csv(archivo_csv_base, parse_dates=['FECHA'])
    df_principal['FECHA'] = df_principal['FECHA'].dt.date
    
    # Carga el reporte de huecos
    df_huecos = pd.read_csv(reporte_huecos_path, parse_dates=['inicio', 'fin'])
    df_huecos_cortos = df_huecos[df_huecos['tipo'] == 'corto'].copy()
    
    df_corregido = df_principal.copy()
    
    if df_huecos_cortos.empty:
        print(f"No se encontraron huecos cortos en {archivo_csv_base}. No se realizaron imputaciones multivariables.")
    else:
        print(f"Iniciando imputación lineal para {len(df_huecos_cortos)} hueco(s) corto(s) multivariables...")

        for _, row in df_huecos_cortos.iterrows():
            columna = row['columna']
            # Convertir fechas a formato date para coincidir con df_corregido
            inicio = row['inicio'].date()
            fin = row['fin'].date()
            
            # Buscamos el índice inicial y final del bloque de datos a interpolar
            idx_inicio = df_corregido[df_corregido['FECHA'] == inicio].index.min()
            idx_fin = df_corregido[df_corregido['FECHA'] == fin].index.max()
            
            if pd.isna(idx_inicio) or pd.isna(idx_fin):
                continue

            rango_a_interpolar = df_corregido.loc[idx_inicio-1 : idx_fin+1, columna]
            
            # Aplicar interpolación lineal
            rango_imputado = rango_a_interpolar.interpolate(method='linear', limit_direction='both')
            
            # Reemplazar los valores imputados en el DataFrame corregido
            df_corregido.loc[idx_inicio-1 : idx_fin+1, columna] = rango_imputado
        
    
    # Nombre del archivo de salida: NR_1234_imputado_corto.csv
    nombre_salida = f'{base_name}_imputado_corto{ext}'
    archivo_salida_final = os.path.join(carpeta_salida, nombre_salida)
    
    df_corregido['FECHA'] = pd.to_datetime(df_corregido['FECHA']) 
    df_corregido.to_csv(archivo_salida_final, index=False, encoding='utf-8')
    
    print(f"Imputación lineal finalizada. Archivo guardado en: {archivo_salida_final}")
    return archivo_salida_final

def imputar_largos(archivo_csv_imputado_corto: str):
    """
    Maneja el proceso de imputación de huecos largos (> 3 días) en el archivo base.
    Delega la imputación al modelo Random Forest definido en RFColLargos.

    Args:
        archivo_csv_base (str): Ruta del archivo CSV generado por imputar_cortos.

    Returns:
        str: Ruta del archivo corregido con los huecos largos imputados.
    """
    
    # Define la ruta del reporte de huecos a partir del archivo imputado_corto 
    dir_base = os.path.dirname(os.path.dirname(archivo_csv_imputado_corto))
    reporte_huecos_path = os.path.join(dir_base, 'huecos_columnas', f'{os.path.basename(archivo_csv_imputado_corto).replace("_imputado_corto", "_HuecosColumnas")}')

    if not os.path.exists(reporte_huecos_path):
        print(f"Error: No se encontró el reporte de huecos en {reporte_huecos_path}")
        return None

    # Cargar el archivo con imputaciones cortas y el reporte de huecos, separando los huecos largos
    df_principal = pd.read_csv(archivo_csv_imputado_corto, parse_dates=['FECHA'])
    df_huecos = pd.read_csv(reporte_huecos_path, parse_dates=['inicio', 'fin'])
    df_huecos_largos = df_huecos[df_huecos['tipo'] == 'largo'].copy()

    if df_huecos_largos.empty:
        print(f"No se encontraron huecos largos en {archivo_csv_imputado_corto}.")
        return archivo_csv_imputado_corto

    print(f"Iniciando imputación de {len(df_huecos_largos)} hueco(s) largo(s) con Random Forest...")

    df_impCorto = df_principal.copy()
    

    # Crear una máscara para excluir los registros de huecos largos
    mask_huecos_largos = pd.Series(False, index=df_impCorto.index)
    for _, row in df_huecos_largos.iterrows():
        mask_huecos_largos |= (df_impCorto['FECHA'] >= row['inicio']) & (df_impCorto['FECHA'] <= row['fin'])

    
    columnas_predictoras = ['PRECIP', 'EVAP', 'TMAX', 'TMIN']
    # Filtrar los datos de entrenamiento excluyendo los huecos largos
    df_entrenamiento = df_impCorto[~mask_huecos_largos].dropna(subset=columnas_predictoras)

    # Entrenar el modelo global
    # columna_objetivo = df_huecos_largos.iloc[0]['columna'] if not df_huecos_largos.empty else None

    rfImputacion.procesar_imputacion(df_entrenamiento,df_impCorto, df_huecos_largos, columnas_predictoras)
    
    rfImputacion.crear_archivo_imputado_rf(dir_base, archivo_csv_imputado_corto, df_impCorto)



    


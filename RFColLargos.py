import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV
import pandas as pd


def entrenar_RF(df: pd.DataFrame, columnas_predictoras: list):
    """
    Entrena un modelo de Random Forest para predecir valores faltantes en múltiples columnas.
    Divide los datos en conjuntos de entrenamiento y validación.

    Args:
        df (pd.DataFrame): DataFrame con datos completos para entrenar el modelo.
        columnas_objetivo (list): Lista de nombres de columnas objetivo.
        columnas_predictoras (list): Lista de columnas predictoras.

    Returns:
        dict: Diccionario con modelos entrenados para cada columna objetivo.
    """
    #param_grid = {
    #    'n_estimators': [100, 200, 300],
    #    'max_depth': [None, 10, 20],
    #    'min_samples_split': [2, 5, 10]
    #}

    modelos_rf = {}

    for columna_objetivo in columnas_predictoras:
        df_entrenamiento = df.dropna(subset=columnas_predictoras)
        X = df_entrenamiento[[col for col in columnas_predictoras if col != columna_objetivo]]
        y = df_entrenamiento[columna_objetivo]

        # Dividir los datos en entrenamiento (80%) y validación (20%)
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

        #grid_search = GridSearchCV(RandomForestRegressor(random_state=42), param_grid, cv=3)
        #grid_search.fit(X_train, y_train)
        #print(f"Mejores parámetros para {columna_objetivo}: {grid_search.best_params_}")

        # Entrenar el modelo de Random Forest con parámetros por defecto
        rf = RandomForestRegressor(random_state=42)
        rf.fit(X_train, y_train)

        # Evaluar el modelo en el conjunto de validación
        y_pred = rf.predict(X_val)
        mse = mean_squared_error(y_val, y_pred)
        print(f"Error cuadrático medio (MSE) en validación para {columna_objetivo}: {mse:.4f}")

        modelos_rf[columna_objetivo] = rf

    return modelos_rf


def imputar_con_random_forest(df_hueco: pd.DataFrame, columnas_predictoras: list, modelos_rf: dict):
    """
    Imputa valores faltantes en un hueco utilizando un modelo de Random Forest.

    Args:
        df_entrenamiento (pd.DataFrame): DataFrame con datos completos para entrenar el modelo.
        df_hueco (pd.DataFrame): DataFrame con los valores faltantes a imputar.
        columnas_objetivo (list): Lista de nombres de columnas objetivo.
        columnas_predictoras (list): Lista de columnas predictoras.
        modelos_rf (dict): Diccionario con modelos entrenados para cada columna objetivo.

    Returns:
        pd.DataFrame: DataFrame con los valores imputados para las columnas objetivo.
    """
    imputaciones = df_hueco.copy()

    for columna_objetivo in columnas_predictoras:
        if columna_objetivo in modelos_rf:
            for _, row in df_hueco.iterrows():
                if pd.isna(row[columna_objetivo]):
                    X_hueco = row[columnas_predictoras].drop(columna_objetivo).values.reshape(1, -1)
                    valor_imputado = modelos_rf[columna_objetivo].predict(X_hueco)[0]
                    valor_imputado = round(valor_imputado, 2)
                    imputaciones.at[row.name, columna_objetivo] = valor_imputado
                    

    
    return imputaciones

def crear_archivo_imputado_rf(dir_base: str, archivo_csv_imputado_corto: str, df_impCorto: pd.DataFrame):
    """
    Crea un archivo CSV con los huecos largos imputados utilizando Random Forest.

    Args:
        dir_base (str): Directorio base donde se guardará el archivo.
        archivo_csv_imputado_corto (str): Ruta del archivo CSV con imputaciones cortas.
        df_impCorto (pd.DataFrame): DataFrame con los datos imputados para huecos cortos y largos.

    Returns:
        str: Ruta del archivo corregido con los huecos largos imputados.
    """
    carpeta_salida = os.path.join(dir_base, 'imputados_rf')
    os.makedirs(carpeta_salida, exist_ok=True)

    archivo_salida = os.path.join(carpeta_salida, f'{os.path.basename(archivo_csv_imputado_corto).replace("_imputado_corto", "_imputado_largo_rf")}')

    df_impCorto.to_csv(archivo_salida, index=False, encoding='utf-8')

    print(f"Archivo con huecos largos imputados guardado en: {archivo_salida}")
    return archivo_salida


def procesar_imputacion(df_entrenamiento: pd.DataFrame,df_impCorto: pd.DataFrame,df_huecos_largos: pd.DataFrame, columnas_predictoras: list):
    """_summary_

    Args:
        df_entrenamiento (pd.DataFrame): DataFrame con datos completos para entrenar el modelo.
        df_impCorto (pd.DataFrame): DataFrame con los datos imputados para huecos cortos.
        df_huecos_largos (pd.DataFrame): DataFrame con los huecos largos a imputar.
        columnas_predictoras (list): Lista de nombres de columnas predictoras.

    """
    columnas_predictoras = ['PRECIP', 'EVAP', 'TMAX', 'TMIN']
    
    if columnas_predictoras is not None:
        modelos_rf = entrenar_RF(df_entrenamiento,columnas_predictoras)
    else:
        print("No se pudo determinar la columna objetivo para el entrenamiento.")
        return None
    
    for _, row in df_huecos_largos.iterrows():
        columna = row['columna']
        inicio = row['inicio']
        fin = row['fin']

        # Filtrar el rango del hueco
        mask_hueco = (df_impCorto['FECHA'] >= inicio) & (df_impCorto['FECHA'] <= fin)
        df_hueco = df_impCorto[mask_hueco]
        
        valores_imputados = imputar_con_random_forest(df_hueco, columnas_predictoras, modelos_rf)

        for columna in columnas_predictoras:
            df_impCorto.loc[mask_hueco, columna] = valores_imputados[columna]




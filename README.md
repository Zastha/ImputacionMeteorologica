# Generación de data sintetica mediante modelos de IA a partir de datos meteorologicos del Servicio Metereologico Nacional (SMN) y Comisión Nacional del Agua (CONAGUA)
Autor: René Zaid Zazueta Rivas
Asesor: Zuriel Dathan Mora Felix

Este proyecto tiene como objetivo desarrollar una evaluación de diversas herramientas basadas en IA para la generación de datos  sinteticos, utiles para la reconstrucción de archivos de datos hidro meteorologicos.
Los datos a analizar son provenientes de la Comisión Nacional del Agua, registrados en la página del Servicio Metereológico Nacional: https://smn.conagua.gob.mx/es/
Se investigará cual será el mejor metodo predictivo para realizar la  generación de data sintetica y predicción de datos de los datos seleccionados.

Cada registro cuenta con los siguientes datos:
Fecha de Registro | Precipitación | Evaporación | Temperatura Máxima | Temperatura Mínima | Fecha Auxiliar

El proyecto estará dividido en 4 fases:

1-. Normalizar Los Datos en un CSV

2-.Detectar Hoyos de datos multivariados y registros faltantes

3-.Imputar datos multivariados faltantes
    a-.Utilizar Interpolación Lineal para hoyos pequeños (1-3 días)
    b-.Utilizar Modelos de Machine Learning para hoyos grandes (4+ días)

4-.Imputar registros/días faltantes 
    a-.Utilizar Interpolación Lineal para hoyos pequeños (1-3 días)
    b-.Utilizar Modelos de Machine Learning para hoyos grandes (4+ días)

5-.Probar y comparar la precisión de cada modelo

## Metodología de Imputación de Datos
<img width="3780" height="2162" alt="Tablero en blanco (1)" src="https://github.com/user-attachments/assets/1d3ab2a5-69fa-44b9-9f57-a5a845064e54" />

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

2-.Detectar   gaps de Fechas faltantes

3-.Imputar los datos faltantes por medio de forecasting y backcasting utilizando registros previos y posteriores

4-.Predecir los registros futuros, con el objetivo de llegar a anticipar 10 años a futuro.

# Imputación Meteorológica en Datos de CONAGUA
Autor: René Zaid Zazueta Rivas
Asesor: Zuriel Dathan Mora Felix

Este proyecto tiene como objetivo crear un imputador de datos faltantes, al igual que un pronosticador de datos futuros.
Los datos a analizar son provenientes de la Comisión Nacional del Agua, registrados en la página del Servicio Metereológico Nacional: https://smn.conagua.gob.mx/es/
Se investigará cual será el mejor metodo predictivo para realizar la imputación y predicción de datos de los datos seleccionados.

Cada registro cuenta con los siguientes datos:
Fecha de Registro | Precipitación | Evaporación | Temperatura Máxima | Temperatura Mínima | Fecha Auxiliar

El proyecto estará dividido en 4 fases:

1-. Normalizar Los Datos en un CSV

2-.Detectar de Fechas con huevos de datos

3-.Imputar los datos faltantes por medio de forecasting y backcasting utilizando registros previos y posteriores

4-.Predecir 

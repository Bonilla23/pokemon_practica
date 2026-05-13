# Metodología CRISP-DM
## Porque?
Escogí CRISP-DM porque permite un desarrollo flexive. Pudiendo volver atras si me equivoco en alguna de las capas.

## Como se aplica
### 1- Compresion del Negocio
Transforma datos crudos de una Pokédex en información para facilitar a los jugadores profesionales

### 2- Comprension de los Datos
Implementamos la estructura ( raw, processed y Curated ) con las carpetas correspondientes, en la carpeta Raw ponemos los archivos en crudo sin tocar nada, despues creamos una copia de los datos en "processed", analizando que todo va bien mediante "Limpieza_datos.py" luego procesamos los datos para su uso ( Analisis_datos.py ), una vez procesados los guardamos en Curated para su visualización

### 3- Modelado
Creamos una carpeta propia para guardar el Modelado, utilizando los Clustering, dividiendoles en 5 tipos, luego lo guardamos en una carpeta aparte llamada "Modelado" he creado otra carpeta para no mezclar la información del modelado con la procesada.

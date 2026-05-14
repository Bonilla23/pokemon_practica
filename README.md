# Metodología CRISP-DM
## Porque?
Escogí CRISP-DM porque permite un desarrollo flexive. Pudiendo volver atras si me equivoco en alguna de las capas.

## Como se aplica
### 1- Compresion del Negocio
Transforma datos crudos de una Pokédex en información para facilitar a los jugadores profesionales

### 2- Compresión de la información
Cosultamos los datos de la base de datos obtenida, un dataset con varios pokemons con sus stats y caracteristicas.

### 3- Preparación de la información
En la carpeta Raw ponemos los archivos en crudo sin tocar nada, despues creamos una copia de los datos en "processed", analizando que todo va bien mediante "Limpieza_datos.py" luego procesamos los datos para su uso ( Analisis_datos.py ), una vez procesados los guardamos en Curated para su visualización

### 4- Modelado
Creamos una carpeta propia para guardar el Modelado, utilizando los Clustering, dividiendoles en 5 tipos, luego lo guardamos en una carpeta aparte llamada "Modelado" he creado otra carpeta para no mezclar la información del modelado con la procesada.

### 5- Evaluación
Analizamos mediante PowerBI si los grupos creados con el modelado en Clustering tiene razon.

# PROYECTO POKÉSPARK: Análisis y Clasificación de Datos con PySpark

Este proyecto implementa una arquitectura de datos moderna para el procesamiento de información de la Pokédex, utilizando PySpark para la ingesta, limpieza, análisis y modelado avanzado.

## 1. Metodología: CRISP-DM
Se ha seleccionado la metodología **CRISP-DM** (Cross-Industry Standard Process for Data Mining) por su enfoque cíclico y flexible:

1.  **Comprensión del Negocio:** El objetivo es transformar datos crudos en insights competitivos para jugadores y analistas, categorizando Pokémon según su rendimiento en combate.
2.  **Comprensión de los Datos:** Exploración del JSON original, identificando anidamientos en estadísticas (`base`) y nombres multilingües.
3.  **Preparación de los Datos:** Limpieza de registros corruptos, aplanado de estructuras anidadas y normalización de nombres de columnas al español.
4.  **Modelado:** Aplicación de Clustering K-Means para segmentar la población Pokémon de forma objetiva.
5.  **Evaluación:** Validación de los grupos obtenidos y su utilidad en el dashboard de Power BI.
6.  **Despliegue:** Entrega de datasets en formato Parquet optimizado para BI.

## 2. Arquitectura de Datos y Almacenamiento
Se utiliza una arquitectura por capas para garantizar la integridad y trazabilidad:

*   **Capa RAW (`Datos/Raw`):** Almacena el JSON original sin modificaciones.
*   **Capa PROCESSED (`Datos/Processed`):** Datos ya limpios, aplanados y tipados. Almacenados en formato **Parquet** para optimizar el almacenamiento y la velocidad de lectura.
*   **Capa CURATED (`Datos/Curated`):** Vistas calculadas listas para consumo (Top Stats, Murallas, Sweepers).
*   **Capa MODELADO (`Datos/Modelado`):** Resultados del modelo de Machine Learning.

## 3. Análisis Avanzado: Interpretación de Clusters (K-Means)
Se han definido 5 clusters basados en las estadísticas base. Tras el análisis, la clasificación se interpreta de la siguiente manera:

*   **Cluster 0 - Básicos o Early-game:** Pokémon con estadísticas defensivas y de ataque bajas, generalmente encontrados al inicio del juego.
*   **Cluster 1 - Atacantes Físicos:** Se caracterizan por un Ataque alto pero una Defensa baja.
*   **Cluster 2 - Defensivos de Apoyo:** Estadísticas de Defensa media-alta y un Ataque medio.
*   **Cluster 3 - Atacantes Especiales:** Pokémon con Ataque medio y Defensa media, centrados en el daño especial.
*   **Cluster 4 - Tanques Pesados:** Poseen una Defensa muy alta, con un Ataque muy variado según el ejemplar.

## 4. Visualización
En la carpeta `PowerBI/` se incluye el archivo `Pokemon.pbix`, el cual conecta con los archivos Parquet de las capas Curated y Modelado para ofrecer una visión interactiva del ecosistema Pokémon.
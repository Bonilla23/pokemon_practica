from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count
import os

# Iniciar sesión
spark = SparkSession.builder.appName("Analisis_Curated").getOrCreate()

try:
    # 1. Leer desde process
    df_processed = spark.read.parquet("Datos/Processed/pokedex_limpia")
    
    # 2. Transformaciones
    df_base = df_processed.withColumn(
        "Total_Stats",
        col("hp") + col("ataque") + col("defensa") + 
        col("ataque_especial") + col("defensa_especial") + col("velocidad")
    ).withColumn(
        "Bulk_Total", 
        col("hp") + col("defensa") + col("defensa_especial")
    )

    # --- DATASETS ESPECÍFICOS ---
    df_top_stats = df_base.select("nombre", "tipos", "Total_Stats").orderBy(col("Total_Stats").desc())
    df_sweepers_fisicos = df_base.filter((col("ataque") >= 100) & (col("velocidad") >= 100)) \
        .select("nombre", col("ataque").alias("Attack"), col("velocidad").alias("Speed"), "tipos")
    df_sweepers_especiales = df_base.filter((col("ataque_especial") >= 100) & (col("velocidad") >= 100)) \
        .select("nombre", col("ataque_especial").alias("Sp_Attack"), col("velocidad").alias("Speed"), "tipos")
    df_murallas = df_base.select("nombre", "Bulk_Total", "tipos").orderBy(col("Bulk_Total").desc())
    df_conteo_tipos = df_base.withColumn("Tipo_Individual", explode(col("tipos"))) \
        .groupBy("Tipo_Individual").agg(count("nombre").alias("Total_Pokemon"))

    # --- DICCIONARIO DE RUTAS ---
    rutas = {
        "top_stats": df_top_stats,
        "sweepers_fisicos": df_sweepers_fisicos,
        "sweepers_especiales": df_sweepers_especiales,
        "murallas": df_murallas,
        "conteo_por_tipo": df_conteo_tipos,
        "full_curated": df_base 
    }

    # --- GUARDADO CON MODO OVERWRITE ---
    for carpeta, dataframe in rutas.items():
        path = f"Datos/Curated/{carpeta}"
        
        # Usamos mode("overwrite") para asegurar que los datos se actualicen
        dataframe.write.mode("overwrite").parquet(path)
        
        print(f"Completado: {path} (Datos actualizados)")

    print("\n--- Proceso finalizado (Modo conservador activo) ---")

except Exception as e:
    print("Error en el proceso de Curated: ", e)
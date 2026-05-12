from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count
import os

# Iniciar sesión
spark = SparkSession.builder.appName("Analisis_Curated_NoBorrar").getOrCreate()

try:
    # 1. Leer desde process
    df_processed = spark.read.parquet("Datos/Processed")
    
    # 2. Transformaciones
    df_base = df_processed.withColumn(
        "Total_Stats",
        col("base.HP") + col("base.Attack") + col("base.Defense") + 
        col("base.`Sp. Attack`") + col("base.`Sp. Defense`") + col("base.Speed")
    ).withColumn(
        "Bulk_Total", 
        col("base.HP") + col("base.Defense") + col("base.`Sp. Defense`")
    )

    # --- DATASETS ESPECÍFICOS ---
    df_top_stats = df_base.select("name", "type", "Total_Stats").orderBy(col("Total_Stats").desc())
    df_sweepers_fisicos = df_base.filter((col("base.Attack") >= 100) & (col("base.Speed") >= 100)) \
        .select("name", col("base.Attack").alias("Attack"), col("base.Speed").alias("Speed"), "type")
    df_sweepers_especiales = df_base.filter((col("base.`Sp. Attack`") >= 100) & (col("base.Speed") >= 100)) \
        .select("name", col("base.`Sp. Attack`").alias("Sp_Attack"), col("base.Speed").alias("Speed"), "type")
    df_murallas = df_base.select("name", "Bulk_Total", "type").orderBy(col("Bulk_Total").desc())
    df_conteo_tipos = df_base.withColumn("Tipo_Individual", explode(col("type"))) \
        .groupBy("Tipo_Individual").agg(count("name").alias("Total_Pokemon"))

    # --- DICCIONARIO DE RUTAS ---
    rutas = {
        "top_stats": df_top_stats,
        "sweepers_fisicos": df_sweepers_fisicos,
        "sweepers_especiales": df_sweepers_especiales,
        "murallas": df_murallas,
        "conteo_por_tipo": df_conteo_tipos,
        "full_curated": df_base 
    }

    # --- GUARDADO CON MODO IGNORE ---
    for carpeta, dataframe in rutas.items():
        path = f"Datos/Curated/{carpeta}"
        
        # Usamos mode("ignore") para que no intente borrar nada
        # Si la carpeta existe, Spark simplemente no escribirá y pasará a la siguiente
        dataframe.write.mode("ignore").parquet(path)
        
        if os.path.exists(path):
            print(f"Completado/Omitido: {path} (La carpeta ya existe o ha sido creada)")
        else:
            print(f"Error: No se pudo crear la carpeta {path}")

    print("\n--- Proceso finalizado (Modo conservador activo) ---")

except Exception as e:
    print("Error en el proceso de Curated: ", e)
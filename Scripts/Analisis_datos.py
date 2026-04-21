from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Iniciar sesión
spark = SparkSession.builder.appName("Analisis_Curated").getOrCreate()

try:
    # 1. Leer desde process
    df_processed = spark.read.parquet("Datos/Processed")
    
    # 2. Calculamos el "Poder Total" sumando las estadísticas base
    df_curated = df_processed.withColumn(
        "Total_Stats",
        col("base.HP") + col("base.Attack") + col("base.Defense") + 
        col("base.`Sp. Attack`") + col("base.`Sp. Defense`") + col("base.Speed")
    )
    

    df_curated = df_curated.withColumn(
        "Categoria",
        (col("Total_Stats") > 500).cast("string") 
    )
    
    print("Previsualización de los datos en capa Curated:")
    df_curated.select("id", "name", "Total_Stats", "Categoria").show(5)
    
    # 3. Guardamos en Curated
    df_curated.write.mode("overwrite").parquet("Datos/Curated")
    print("Datos transformados y guardados exitosamente en Datos/Curated")

except Exception as e:
    print("Error en el proceso de Curated: ", e)
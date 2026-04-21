from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lower
from functools import reduce

spark = SparkSession.builder.appName("Limpieza_datos").getOrCreate()

try:
    #----------Saber que nos llega----------
    df = spark.read.option("multiline","true").json("Datos/Raw/pokedex.json")
    # Limitar a 10 registros
    df_limit = df.limit(10)
    # Mostrar los datos
    df_limit.show(truncate=False)
    df_limit.printSchema()
    print()
    #-----------------------------------------------#
    #----------Comprobar si el ID se repite----------
    print("Si el ID ser repite aparece aqui:")
    df.groupBy("id").count().filter("count > 1").show()

    #----------Comprobar si hay nulos---------------
    print("Si hay algun nulo aparece aqui: ")
    df.filter(
        reduce(lambda x, y: x | y, (col(c).isNull() for c in df.columns))
    ).show(truncate=False)

    #----------Borrar el chino y el Frances---------
    print("Borrado del idioma chino, frances y japones")
    df_limpio = df.withColumn(
        "name",
    col("name").dropFields("french", "chinese", "japanese")
    )

    #----------Como borre todos menos ingles cambiamos a nombre solo---#
    df_limpio = df_limpio.withColumn("name", col("name.english"))

    #----------Comprobar que en la base ninguno es 0-------#
    df_ceros = df_limpio.filter(
        (col("base.Attack") == 0) |
        (col("base.Defense") == 0) |
        (col("base.HP") == 0) |
        (col("base.`Sp. Attack`") == 0) |   
        (col("base.`Sp. Defense`") == 0) | 
        (col("base.Speed") == 0)
    )

    # Mostramos los resultados (si está vacío, significa que no hay ningún 0)
    print("Registros con alguna estadística en 0:")
    df_ceros.show()

    #----------Comprobación----------------
    print("----------------------------")
    df_limpio_limit = df_limpio.limit(5)
    df_limpio_limit.show()

    #----------Guardar los datos limpios---
    df_limpio.write.mode("overwrite").parquet("Datos/Processed")
except Exception as e:
    print("Error al leer el fichero: ", e)
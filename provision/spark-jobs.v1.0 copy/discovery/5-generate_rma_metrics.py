from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, count

# Initialiser Spark
spark = SparkSession.builder.appName("RMA").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Lire la table pivot
pivot_df = spark.read.parquet("hdfs://localhost:9000/datalake/pivot/pivot_table")

# Calculer les consultations par mois et service
consultations = pivot_df.groupBy(
    date_trunc("month", col("date")).alias("month"),
    col("ward_name")
).agg(count("*").alias("consultation_count"))

# Calculer les hospitalisations par mois et service
hospitalizations = pivot_df.groupBy(
    date_trunc("month", col("admission_date")).alias("month"),
    col("ward_name")
).agg(count("*").alias("hospitalization_count"))

# Combiner les métriques
rma_metrics = consultations.join(hospitalizations, ["month", "ward_name"], "outer").fillna(0)

# Afficher un aperçu
rma_metrics.show(10, truncate=False)

# Sauvegarder les résultats
rma_metrics.write.mode("overwrite").csv("hdfs://localhost:9000/datalake/rma/consultations_by_month_ward")

# Arrêter Spark
spark.stop()
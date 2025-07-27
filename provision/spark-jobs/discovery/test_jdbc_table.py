from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestJDBC") \
    .config("spark.jars", "/home/vagrant/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

url = "jdbc:postgresql://127.0.0.1:15432/mavis_notheme"
properties = {
    "user": "mirth_user",
    "password": "00000!",
    "driver": "org.postgresql.Driver",
    "ssl": "false"
}

try:
    df = spark.read.jdbc(
        url=url,
        table="(SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' AND table_type IN ('BASE TABLE', 'VIEW')) t",
        properties=properties
    )
    tables = df.collect()
    print(f"Nombre de tables trouvées : {len(tables)}")
    for row in tables:
        print(f"Table: {row['table_name']}, Type: {row['table_type']}")
except Exception as e:
    print(f"Erreur : {str(e)}")

spark.stop()
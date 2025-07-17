from pyspark.sql import SparkSession
import json
from sshtunnel import SSHTunnelForwarder

def discover_postgres(source, spark):
    ssh_cfg = source.get("ssh")
    db_cfg = source["db"]
    tables_info = []

    with SSHTunnelForwarder(
        (ssh_cfg["host"], ssh_cfg["port"]),
        ssh_username=ssh_cfg["user"],
        ssh_password=ssh_cfg["password"],
        remote_bind_address=(db_cfg["host"], db_cfg["port"])
    ) as tunnel:
        print(f"Tunnel SSH ouvert sur le port local {tunnel.local_bind_port}")
        url = f"jdbc:postgresql://localhost:{tunnel.local_bind_port}/{db_cfg['database']}"
        df_tables = spark.read.jdbc(
            url=url,
            table="(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'hms_patient_%') t",
            properties={"user": db_cfg["user"], "password": db_cfg["password"], "driver": "org.postgresql.Driver"}
        )
        tables = [row["table_name"] for row in df_tables.collect()]

        for table in tables:
            df_cols = spark.read.jdbc(
                url=url,
                table=f"(SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position) t",
                properties={"user": db_cfg["user"], "password": db_cfg["password"], "driver": "org.postgresql.Driver"}
            )
            columns = [{"name": row["column_name"], "type": row["data_type"]} for row in df_cols.collect()]
            tables_info.append({"table_name": table, "columns": columns})

    return tables_info

def discover_excel(source, spark):
    df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", source["options"]["header"]) \
        .option("inferSchema", source["options"]["inferSchema"]) \
        .load(source["path"])
    columns = [{"name": col, "type": str(df.schema[col].dataType)} for col in df.columns]
    return [{"table_name": "sheet1", "columns": columns}]  # Supposant une seule feuille

def main():
    spark = SparkSession.builder \
        .appName("PivotSchemaDiscovery") \
        .master("local[*]") \
        .config("spark.jars", ",".join([
            "/home/jonah/spark-3.5.0-bin-hadoop3/jars/postgresql-42.7.3.jar",
            "/home/jonah/spark-3.5.0-bin-hadoop3/jars/spark-excel_2.12-3.5.1_0.20.4.jar",
            "/home/jonah/spark-3.5.0-bin-hadoop3/jars/poi-5.2.5.jar",
            "/home/jonah/spark-3.5.0-bin-hadoop3/jars/poi-ooxml-5.2.5.jar",
            "/home/jonah/spark-3.5.0-bin-hadoop3/jars/poi-ooxml-lite-5.2.5.jar",
            "/home/jonah/spark-3.5.0-bin-hadoop3/jars/xmlbeans-5.2.1.jar",
            "/home/jonah/spark-3.5.0-bin-hadoop3/jars/commons-compress-1.26.2.jar",
            "/home/jonah/spark-3.5.0-bin-hadoop3/jars/commons-collections4-4.4.jar"
        ])).getOrCreate()

    with open("config/data_sources.json") as f:
        sources = json.load(f)

    discovery_report = {}

    for source in sources:
        print(f"\nDécouverte pour {source['name']} ({source['type']})")
        try:
            if source["type"] == "postgres":
                discovery_report[source["name"]] = discover_postgres(source, spark)
            elif source["type"] == "excel":
                discovery_report[source["name"]] = discover_excel(source, spark)
        except Exception as e:
            discovery_report[source["name"]] = {"error": str(e)}
            print(f"❌ Erreur : {e}")

    with open("spark_jobs/discovery/discovery_report.json", "w") as f:
        json.dump(discovery_report, f, indent=2)

    print("\n✅ Rapport de découverte généré : discovery_report.json")
    spark.stop()

if __name__ == "__main__":
    main()
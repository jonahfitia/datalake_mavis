from sshtunnel import SSHTunnelForwarder
import os
import json
from pyspark.sql import SparkSession
from openpyxl import load_workbook
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/datalake-mavis/provision/.env"))
log_file = os.path.expanduser("~/datalake-mavis/provision/logs/datalake.log")
logging.basicConfig(filename=log_file, level=logging.INFO)

def discover_postgres(source, spark, source_index):
    logging.info(f"Découverte pour {source['name']}")
    ssh_cfg = source.get("ssh")
    db_cfg = source["db"]
    tables_info = []

    try:
        with SSHTunnelForwarder(
            (ssh_cfg["host"], ssh_cfg["port"]),
            ssh_username=ssh_cfg["user"],
            ssh_password=os.getenv(f"SSH_PASSWORD_{source['name']}"),
            remote_bind_address=(db_cfg["host"], db_cfg["port"]),
            # local_bind_address=('0.0.0.0', 5432 + source_index)
            local_bind_address=('127.0.0.1', 6000 + source_index)
        ) as tunnel:
            url = f"jdbc:postgresql://localhost:{tunnel.local_bind_port}/{db_cfg['database']}"
            properties = {
                "user": db_cfg["user"],
                "password": os.getenv(f"POSTGRES_PASSWORD_{source['name']}"),
                "driver": "org.postgresql.Driver",
                "ssl": "true",
                "sslmode": "require"
            }

            # Récupérer les tables
            df_tables = spark.read.jdbc(
                url=url,
                table="(SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' AND table_type IN ('BASE TABLE', 'VIEW')) t",
                properties=properties
            )
            tables = [{"table_name": row["table_name"], "table_type": row["table_type"]} for row in df_tables.collect()]

            for table in tables[:1000]:  # Limiter à 1000 tables par base
                table_name = table["table_name"]
                try:
                    df_cols = spark.read.jdbc(
                        url=url,
                        table=f"(SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position) t",
                        properties=properties
                    )
                    columns = [{"name": row["column_name"], "type": str(row["data_type"])} for row in df_cols.collect()]
                    df_data = spark.read.jdbc(url=url, table=table_name, properties=properties)
                    sample_data = df_data.limit(10).collect()  # Échantillon de 10 lignes
                    df_data.write.mode("overwrite").parquet(f"hdfs://localhost:9000/datalake/raw/{source['name']}/{table_name}")
                    tables_info.append({
                        "table_name": table_name,
                        "table_type": table_type,
                        "columns": columns,
                        "row_count": df_data.count(),
                        "sample_data": [row.asDict() for row in sample_data]
                    })
                    logging.info(f"Table {table_name} processed: {len(columns)} columns, {df_data.count()} rows")
                except Exception as e:
                    logging.error(f"Error processing table {table_name}: {str(e)}")
                    continue
    except Exception as e:
        logging.error(f"Error connecting to {source['name']}: {str(e)}")
        return tables_info

    return tables_info

def discover_excel(source, spark):
    df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", source["options"]["header"]) \
        .option("inferSchema", source["options"]["inferSchema"]) \
        .option("dataAddress", "PatientList") \
        .load(source["path"])
    columns = [{"name": col, "type": str(df.schema[col].dataType)} for col in df.columns]
    return [{"table_name": "PatientList", "columns": columns}]

def main():
    # Configuration Spark avec vos JARs explicites
    jars = [
        "~/spark-3.5.0-bin-hadoop3/jars/postgresql-42.7.3.jar",
        "~/spark-3.5.0-bin-hadoop3/jars/spark-excel_2.12-3.5.1_0.20.4.jar",
        "~/spark-3.5.0-bin-hadoop3/jars/poi-5.2.5.jar",
        "~/spark-3.5.0-bin-hadoop3/jars/poi-ooxml-5.2.5.jar",
        "~/spark-3.5.0-bin-hadoop3/jars/poi-ooxml-lite-5.2.5.jar",
        "~/spark-3.5.0-bin-hadoop3/jars/xmlbeans-5.1.1.jar",
        "~/spark-3.5.0-bin-hadoop3/jars/commons-compress-1.26.2.jar",
        "~/spark-3.5.0-bin-hadoop3/jars/commons-collections4-4.4.jar",
        "~/spark-3.5.0-bin-hadoop3/jars/ooxml-schemas-1.4.jar"
    ]
    # Expand ~ to absolute paths
    expanded_jars = [os.path.expanduser(jar) for jar in jars]

    # Configuration Spark avec les JARs
    spark = SparkSession.builder \
        .appName("PivotSchemaDiscovery") \
        .master("local[*]") \
        .config("spark.jars", ",".join(expanded_jars)) \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Charger data_sources.json depuis le dossier synchronisé
    with open("/home/jonah/datalake-mavis/provision/config/data_sources.json") as f:
        sources = json.load(f)

    discovery_report = {}

    # Paralléliser les bases PostgreSQL avec ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_source = {
            executor.submit(discover_postgres, source, spark, i): source
            for i, source in enumerate(sources) if source["type"] == "postgres"
        }
        for future in as_completed(future_to_source):
            source = future_to_source[future]
            try:
                discovery_report[source["name"]] = future.result()
                print(f"✅ Découverte réussie pour {source['name']} (postgres)")
            except Exception as e:
                discovery_report[source["name"]] = {"error": str(e)}
                print(f"❌ Erreur pour {source['name']} (postgres): {str(e)}")
                logging.error(f"Failed for {source['name']}: {str(e)}")

    # Traiter les sources Excel séquentiellement
    for source in sources:
        if source["type"] == "excel":
            try:
                discovery_report[source["name"]] = discover_excel(source, spark)
                print(f"✅ Découverte réussie pour {source['name']} (excel)")
            except Exception as e:
                discovery_report[source["name"]] = {"error": str(e)}
                print(f"❌ Erreur pour {source['name']} (excel): {str(e)}")
                logging.error(f"Failed for {source['name']}: {str(e)}")

    # Créer le dossier de sortie et enregistrer le rapport
    output_dir = "~/datalake-mavis/provision/spark-jobs/discovery"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "discovery_report.json")
    with open(output_file, "w") as f:
        json.dump(discovery_report, f, indent=2)

    print(f"\n✅ Rapport de découverte généré : {output_file}")
    spark.stop()

if __name__ == "__main__":
    main()
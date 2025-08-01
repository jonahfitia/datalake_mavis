from sshtunnel import SSHTunnelForwarder
import os
import json
from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer, util
import logging
import datetime
import decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration des logs
logging.basicConfig(
    filename="/home/vagrant/datalake-mavis/provision/logs/datalake.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialiser le modèle TALN (SentenceTransformers)
try:
    model = SentenceTransformer('all-MiniLM-L6-v2')
    logging.info("Modèle SentenceTransformers chargé avec succès")
except Exception as e:
    logging.error(f"Erreur lors du chargement du modèle TALN: {str(e)}")
    model = None

# Termes clés pour FHIR/RMA
FHIR_KEYWORDS = [
    "patient", "person", "individual", "diagnosis", "condition", "hospitalization",
    "encounter", "consultation", "admission", "discharge", "treatment", "procedure"
]

def json_serial(obj):
    """JSON serializer pour objets non sérialisables."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return str(obj)

def select_relevant_tables(tables, columns_data, threshold=0.6):
    """Utilise le TALN pour sélectionner les tables pertinentes."""
    if not model:
        logging.warning("Modèle TALN non disponible, retour aux filtres statiques")
        return [t["table_name"] for t in tables]
    
    relevant_tables = []
    keyword_embeddings = model.encode(FHIR_KEYWORDS, convert_to_tensor=True)
    
    for table in tables:
        table_name = table["table_name"]
        table_columns = [col["column_name"] for col in columns_data if col["table_name"] == table_name]
        text_to_analyze = [table_name] + table_columns
        text_embeddings = model.encode(text_to_analyze, convert_to_tensor=True)
        similarities = util.cos_sim(text_embeddings, keyword_embeddings)
        max_similarity = similarities.max().item()
        
        logging.info(f"Table {table_name}: Similarité max = {max_similarity:.2f}")
        if max_similarity >= threshold:
            relevant_tables.append(table_name)
    
    logging.info(f"Tables pertinentes sélectionnées: {relevant_tables}")
    return relevant_tables

def discover_postgres(source, spark, source_index):
    """Explore une base PostgreSQL et génère les métadonnées des tables pertinentes."""
    logging.info(f"Début de la découverte pour {source['name']}")
    ssh_cfg = source.get("ssh", {})  # Configuration SSH par défaut vide
    db_cfg = source["db"]
    tables_info = []
    cache_path = f"/home/vagrant/datalake-mavis/provision/spark-jobs/metadata/{source['name']}_discovery_report.json"

    # Vérifier le cache
    if os.path.exists(cache_path):
        logging.info(f"Cache trouvé pour {source['name']}: {cache_path}")
        with open(cache_path, "r") as f:
            return json.load(f)

    try:
        # Récupérer les identifiants
        ssh_password = os.getenv(f"SSH_PASSWORD_{source['name']}", ssh_cfg.get("password") if ssh_cfg else None)
        ssh_private_key = ssh_cfg.get("private_key")  # Chemin de la clé privée
        db_password = os.getenv(f"POSTGRES_PASSWORD_{source['name']}", db_cfg.get("password"))
        
        logging.info(f"SSH Password défini: {bool(ssh_password)}")
        logging.info(f"SSH Private Key défini: {bool(ssh_private_key)}")
        logging.info(f"DB Password défini: {bool(db_password)}")

        # Valider les identifiants
        if not db_password:
            raise ValueError(f"Mot de passe de la base de données manquant pour {source['name']}")
        
        # Configurer le tunnel SSH si nécessaire
        tunnel = None
        if ssh_cfg and ssh_cfg.get("host") and ssh_cfg.get("port") and ssh_cfg.get("user"):
            try:
                tunnel_args = {
                    "ssh_address_or_host": (ssh_cfg["host"], ssh_cfg["port"]),
                    "ssh_username": ssh_cfg["user"],
                    "remote_bind_address": (db_cfg["host"], db_cfg["port"]),
                    "local_bind_address": ('127.0.0.1', 15432 + source_index)
                }
                
                # Prioriser la clé privée si disponible
                if ssh_private_key and os.path.exists(ssh_private_key):
                    tunnel_args["ssh_private_key"] = ssh_private_key
                    logging.info(f"Utilisation de la clé privée SSH: {ssh_private_key}")
                elif ssh_password:
                    tunnel_args["ssh_password"] = ssh_password
                    logging.info(f"Utilisation du mot de passe SSH pour {source['name']}")
                else:
                    raise ValueError(f"Aucune méthode d'authentification SSH valide pour {source['name']}: ni clé privée ni mot de passe")

                tunnel = SSHTunnelForwarder(**tunnel_args)
                tunnel.start()
                logging.info(f"Tunnel SSH établi sur port local {tunnel.local_bind_port}")
                url = f"jdbc:postgresql://127.0.0.1:{tunnel.local_bind_port}/{db_cfg['database']}"
            except Exception as e:
                logging.error(f"Erreur lors de l'établissement du tunnel SSH pour {source['name']}: {str(e)}")
                tables_info.append({"source_name": source["name"], "error": f"Échec du tunnel SSH: {str(e)}"})
                return tables_info  # Retourner l'erreur sans bloquer
        else:
            url = f"jdbc:postgresql://{db_cfg['host']}:{db_cfg['port']}/{db_cfg['database']}"
            logging.info(f"Connexion directe sans SSH pour {source['name']}")
            
        # Propriétés de connexion JDBC
        properties = {
            "user": db_cfg["user"],
            "password": db_password,
            "driver": "org.postgresql.Driver",
            "ssl": "false"
        }

        # Récupérer toutes les tables pour TALN
        tables_query = """
            (SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type IN ('BASE TABLE', 'VIEW')
            ) t
        """
        logging.info("Exécution de la requête pour récupérer toutes les tables...")
        df_tables = spark.read.jdbc(url=url, table=tables_query, properties=properties)
        tables = [{"table_name": row["table_name"], "table_type": row["table_type"]} for row in df_tables.collect()]
        logging.info(f"Nombre total de tables trouvées: {len(tables)}")

        # Récupérer toutes les colonnes pour TALN
        columns_query = """
            (SELECT table_name, column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public'
            ) t
        """
        df_columns = spark.read.jdbc(url=url, table=columns_query, properties=properties)
        columns_data = [{"table_name": row["table_name"], "column_name": row["column_name"]} for row in df_columns.collect()]

        # Sélectionner les tables pertinentes
        tables_to_include = source.get("tables_to_include", [])
        if tables_to_include:
            relevant_tables = [t["table_name"] for t in tables if t["table_name"] in tables_to_include]
        else:
            relevant_tables = select_relevant_tables(tables, columns_data, threshold=0.6)
        logging.info(f"Tables pertinentes après TALN: {relevant_tables}")

        # Traiter les tables pertinentes par lots
        batch_size = 100
        for i in range(0, len(relevant_tables), batch_size):
            batch_tables = relevant_tables[i:i + batch_size]
            logging.info(f"Traitement du lot {i//batch_size + 1}/{len(relevant_tables)//batch_size + 1}")
            for table_name in batch_tables:
                logging.info(f"Traitement de la table: {table_name}")
                try:
                    # Récupérer les colonnes
                    df_cols = spark.read.jdbc(
                        url=url,
                        table=f"(SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position) t",
                        properties=properties
                    )
                    columns = [{"name": row["column_name"], "type": str(row["data_type"])} for row in df_cols.collect()]
                    logging.info(f"Colonnes pour {table_name}: {columns}")

                    # Récupérer un échantillon de données
                    df_data = spark.read.jdbc(url=url, table=table_name, properties=properties)
                    sample_data = df_data.limit(10).collect()

                    # Enregistrer les données brutes en Parquet
                    df_data.write.mode("overwrite").parquet(f"hdfs://localhost:9000/datalake/raw/{source['name']}/{table_name}")

                    # Ajouter les métadonnées
                    tables_info.append({
                        "source_name": source["name"],
                        "table_name": table_name,
                        "table_type": next(t["table_type"] for t in tables if t["table_name"] == table_name),
                        "columns": columns,
                        "row_count": df_data.count(),
                        "sample_data": [row.asDict() for row in sample_data]
                    })
                    logging.info(f"Table {table_name} traitée: {len(columns)} colonnes, {df_data.count()} lignes")
                except Exception as e:
                    logging.error(f"Erreur lors du traitement de la table {table_name}: {str(e)}")
                    continue

        # Fermer le tunnel SSH si utilisé
        if tunnel:
            tunnel.stop()
            logging.info(f"Tunnel SSH fermé pour {source['name']}")

    except Exception as e:
        logging.error(f"Erreur de connexion à {source['name']}: {str(e)}")
        tables_info.append({"source_name": source["name"], "error": str(e)})

    # Enregistrer dans le cache
    with open(cache_path, "w") as f:
        json.dump(tables_info, f, indent=2, default=json_serial)
    logging.info(f"Cache enregistré pour {source['name']}: {cache_path}")

    return tables_info

def discover_excel(source, spark):
    """Explore une source Excel et génère les métadonnées."""
    try:
        df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", source["options"]["header"]) \
            .option("inferSchema", source["options"]["inferSchema"]) \
            .option("dataAddress", "PatientList") \
            .load(source["path"])
        columns = [{"name": col, "type": str(df.schema[col].dataType)} for col in df.columns]
        sample_data = df.limit(10).collect()
        df.write.mode("overwrite").parquet(f"hdfs://localhost:9000/datalake/raw/{source['name']}/PatientList")
        return [{
            "source_name": source["name"],
            "table_name": "PatientList",
            "table_type": "EXCEL",
            "columns": columns,
            "row_count": df.count(),
            "sample_data": [row.asDict() for row in sample_data]
        }]
    except Exception as e:
        logging.error(f"Erreur lors du traitement de la source Excel {source['name']}: {str(e)}")
        return [{"source_name": source["name"], "error": str(e)}]

def main():
    # Configuration Spark
    spark = SparkSession.builder \
        .appName("PivotSchemaDiscovery") \
        .master("local[*]") \
        .config("spark.jars", ",".join([
            "/home/vagrant/spark/jars/postgresql-42.7.3.jar",
            "/home/vagrant/spark/jars/spark-excel_2.12-3.5.0_0.20.3.jar",
            "/home/vagrant/spark/jars/poi-5.2.3.jar",
            "/home/vagrant/spark/jars/poi-ooxml-5.2.3.jar",
            "/home/vagrant/spark/jars/commons-collections4-4.4.jar",
            "/home/vagrant/spark/jars/xmlbeans-5.1.1.jar",
            "/home/vagrant/spark/jars/ooxml-schemas-1.4.jar",
            "/home/vagrant/spark/jars/commons-compress-1.21.jar",
            "/home/vagrant/spark/jars/stax-api-1.0.1.jar"
        ])) \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # Charger la configuration
    config_path = "/home/vagrant/datalake-mavis/provision/config/data_sources.json"
    with open(config_path) as f:
        sources = json.load(f)

    discovery_report = {}

    # Paralléliser les bases PostgreSQL
    with ThreadPoolExecutor(max_workers=4) as executor:  # Réduit à 4 pour éviter la surcharge
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
                logging.error(f"Échec pour {source['name']}: {str(e)}")

    # Traiter les sources Excel
    for source in sources:
        if source["type"] == "excel":
            try:
                discovery_report[source["name"]] = discover_excel(source, spark)
                print(f"✅ Découverte réussie pour {source['name']} (excel)")
            except Exception as e:
                discovery_report[source["name"]] = {"error": str(e)}
                print(f"❌ Erreur pour {source['name']} (excel): {str(e)}")
                logging.error(f"Échec pour {source['name']}: {str(e)}")

    # Enregistrer le rapport global
    output_dir = "/home/vagrant/datalake-mavis/provision/spark-jobs/discovery"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "discovery_report.json")
    with open(output_file, "w") as f:
        json.dump(discovery_report, f, indent=2, default=json_serial)

    print(f"\n✅ Rapport de découverte généré: {output_file}")
    spark.stop()

if __name__ == "__main__":
    main()
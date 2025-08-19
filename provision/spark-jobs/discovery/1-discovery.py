from sshtunnel import SSHTunnelForwarder
import os
import json
import socket
import paramiko
from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer, util
import logging
import datetime
import decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
from retrying import retry
import time

# Configuration des logs
logging.basicConfig(
    filename="/home/vagrant/datalake-mavis/provision/logs/datalake.log",
    level=logging.DEBUG,  # Increased verbosity
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialiser le modèle TALN
try:
    model = SentenceTransformer('all-MiniLM-L6-v2')
    logging.info("Modèle SentenceTransformers chargé avec succès")
except Exception as e:
    logging.error(f"Erreur lors du chargement du modèle TALN: {str(e)}")
    model = None

FHIR_KEYWORDS = [
    "patient", "evaluation", "hospitalization", "ward", "disease", "diagnosis",
    "treatment", "appointment", "clinical", "medical", "laboratory", "imaging",
    "vaccination", "surgery", "medication", "health_service", "perinatal"
]

def json_serial(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return str(obj)

def select_relevant_tables(tables, columns_data, threshold=0.80, disable_taln=False):
    if disable_taln or not model:
        logging.warning("Modèle TALN désactivé ou non disponible, utilisation de filtres statiques")
        return [t["table_name"] for t in tables if any(kw in t["table_name"].lower() for kw in FHIR_KEYWORDS)]
    
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

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def test_ssh_connection(host, port, user, private_key=None, password=None, remote_host="localhost", remote_port=5432, timeout=10):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        if result != 0:
            error_msg = f"Échec de la connexion réseau à {host}:{port}, erreur code: {result} ({socket.errorTab.get(result, 'Erreur inconnue')})"
            logging.error(error_msg)
            raise Exception(error_msg)
        logging.info(f"Connexion réseau réussie à {host}:{port}")
    except Exception as e:
        error_msg = f"Erreur lors du test de connexion réseau à {host}:{port}: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    if not password and not private_key:
        logging.error(f"Aucune méthode d'authentification SSH fournie pour {host}:{port}")
        raise ValueError("Aucune méthode d'authentification SSH fournie")
    
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if private_key:
            logging.info(f"Tentative d'authentification SSH avec clé privée pour {host}:{port}")
            ssh.connect(
                host, port=port, username=user, key_filename=private_key, timeout=timeout,
                allow_agent=False, look_for_keys=False
            )
            logging.info(f"Authentification SSH réussie avec clé privée pour {host}:{port}")
        elif password:
            logging.info(f"Tentative d'authentification SSH avec mot de passe pour {host}:{port}")
            ssh.connect(
                host, port=port, username=user, password=password, timeout=timeout,
                allow_agent=False, look_for_keys=False
            )
            logging.info(f"Authentification SSH réussie avec mot de passe pour {host}:{port}")
        else:
            logging.error(f"Clé privée SSH invalide ou non spécifiée pour {host}:{port}")
            raise ValueError("Clé privée SSH invalide ou non spécifiée")
        
        try:
            transport = ssh.get_transport()
            channel = transport.open_channel("direct-tcpip", (remote_host, remote_port), ("127.0.0.1", 0))
            channel.close()
            logging.info(f"Accès au port distant {remote_host}:{remote_port} via SSH réussi")
        except Exception as e:
            logging.error(f"Échec de l'accès au port distant {remote_host}:{remote_port}: {str(e)}")
            raise Exception(f"Échec de l'accès au port distant: {str(e)}")
        
        ssh.close()
        return True, None
    except Exception as e:
        logging.error(f"Échec de l'authentification SSH pour {host}:{port}: {str(e)}")
        raise Exception(f"Échec de l'authentification SSH: {str(e)}")

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def discover_postgres(source, spark, source_index):
    source_name = source["name"]
    logging.info(f"Début de la découverte pour {source_name}")
    ssh_cfg = source.get("ssh", {}) or {}
    db_cfg = source["db"]
    tables_info = []
    failed_tables = []
    cache_path = f"/home/vagrant/datalake-mavis/provision/spark-jobs/metadata/{source_name}_discovery_report.json"
    failed_tables_path = f"/home/vagrant/datalake-mavis/provision/spark-jobs/metadata/{source_name}_failed_tables.json"

    # Cache
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Erreur lors du chargement du cache pour {source_name}: {str(e)}")

    os.makedirs(os.path.dirname(cache_path), exist_ok=True)

    tunnel = None
    try:
        # Secrets (priorité aux variables d'env)
        ssh_password = os.getenv(f"SSH_PASSWORD_{source_name}", ssh_cfg.get("password"))
        db_password  = os.getenv(f"POSTGRES_PASSWORD_{source_name}", db_cfg.get("password"))

        logging.info(f"SSH Password défini: {bool(ssh_password)}")
        logging.info(f"DB Password défini: {bool(db_password)}")

        if not db_password:
            raise ValueError(f"Mot de passe de la base de données manquant pour {source_name}")

        # Décider du mode de connexion
        use_ssh = bool(ssh_cfg.get("host") and ssh_cfg.get("port") and ssh_cfg.get("user"))

        if use_ssh:
            # Test SSH et accessibilité du port distant
            success, error_msg = test_ssh_connection(
                ssh_cfg["host"], ssh_cfg["port"], ssh_cfg["user"],
                password=ssh_password, remote_host=db_cfg["host"], remote_port=db_cfg["port"]
            )
            if not success:
                tables_info.append({"source_name": source_name, "error": error_msg})
                return tables_info

            # Démarrer le tunnel (laisser le port local en auto pour éviter les conflits)
            tunnel = SSHTunnelForwarder(
                ssh_address_or_host=(ssh_cfg["host"], ssh_cfg["port"]),
                ssh_username=ssh_cfg["user"],
                ssh_password=ssh_password,
                remote_bind_address=(db_cfg["host"], db_cfg["port"]),
                local_bind_address=("127.0.0.1", 0),  # port auto
                set_keepalive=60
            )
            tunnel.start()
            local_port = tunnel.local_bind_port
            logging.info(f"Tunnel SSH établi sur port local {local_port}")

            # Test du port local du tunnel
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10)
                sock.connect(("127.0.0.1", local_port))
                sock.close()
                logging.info(f"Connexion au port local {local_port} réussie")
            except Exception as e:
                logging.error(f"Échec de la connexion au port local {local_port}: {str(e)}")
                raise

            jdbc_host, jdbc_port = "127.0.0.1", local_port

        else:
            # Connexion directe → tester l’hôte/port de la DB
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10)
                sock.connect((db_cfg["host"], db_cfg["port"]))
                sock.close()
                logging.info(f"Connexion directe réussie à {db_cfg['host']}:{db_cfg['port']}")
            except Exception as e:
                logging.error(f"Échec de la connexion directe à {db_cfg['host']}:{db_cfg['port']}: {str(e)}")
                raise

            jdbc_host, jdbc_port = db_cfg["host"], db_cfg["port"]

        # URL JDBC selon le mode
        url = (
            f"jdbc:postgresql://{jdbc_host}:{jdbc_port}/{db_cfg['database']}"
            f"?socketTimeout=600&connectTimeout=120&sslmode=disable&loglevel=2"
        )

        properties = {
            "user": db_cfg["user"],
            "password": db_password,
            "driver": "org.postgresql.Driver",
            "connectTimeout": "300",
            "socketTimeout": "900",
        }

        # --- Découverte des tables
        tables_query = """
            (SELECT table_name, table_type 
             FROM information_schema.tables 
             WHERE table_schema = 'public' 
               AND table_type IN ('BASE TABLE', 'VIEW')) t
        """
        logging.info("Exécution de la requête pour récupérer toutes les tables...")
        df_tables = spark.read.jdbc(url=url, table=tables_query, properties=properties)
        tables = [{"table_name": r["table_name"], "table_type": r["table_type"]} for r in df_tables.collect()]
        logging.info(f"Nombre total de tables trouvées: {len(tables)}")

        columns_query = """
            (SELECT table_name, column_name
             FROM information_schema.columns 
             WHERE table_schema = 'public') t
        """
        df_columns = spark.read.jdbc(url=url, table=columns_query, properties=properties)
        columns_data = [{"table_name": r["table_name"], "column_name": r["column_name"]} for r in df_columns.collect()]

        # Filtrage TALN / liste d'inclusion
        tables_to_include = source.get("tables_to_include", []) or []
        disable_taln = source.get("disable_taln", False)
        if tables_to_include:
            relevant_tables = [t["table_name"] for t in tables if t["table_name"] in tables_to_include]
        else:
            relevant_tables = select_relevant_tables(tables, columns_data, threshold=0.6, disable_taln=disable_taln)
        logging.info(f"Tables pertinentes après TALN: {relevant_tables}")

        # --- Parcours des tables pertinentes
        batch_size = 20
        for i in range(0, len(relevant_tables), batch_size):
            batch_tables = relevant_tables[i:i + batch_size]
            logging.info(f"Traitement du lot {i//batch_size + 1}/{(len(relevant_tables)-1)//batch_size + 1}")
            for table_name in batch_tables:
                logging.info(f"Traitement de la table: {table_name}")
                try:
                    # Si tunnel utilisé, s'assurer qu'il est actif
                    if tunnel and not tunnel.is_active:
                        logging.error(f"Tunnel SSH inactif pour {table_name}, redémarrage…")
                        tunnel.start()
                        logging.info("Tunnel SSH redémarré")

                    time.sleep(1)

                    # Colonnes
                    df_cols = spark.read.jdbc(
                        url=url,
                        table=f"(SELECT column_name, data_type FROM information_schema.columns "
                              f"WHERE table_schema='public' AND table_name = '{table_name}' "
                              f"ORDER BY ordinal_position) t",
                        properties=properties
                    )
                    columns = [{"name": r["column_name"], "type": str(r["data_type"])} for r in df_cols.collect()]
                    logging.info(f"Colonnes pour {table_name}: {columns}")

                    # Estimation/compte des lignes
                    count_query = f"""
                        (SELECT CASE 
                            WHEN reltuples > 100000 THEN reltuples::bigint 
                            ELSE (SELECT COUNT(*) FROM public."{table_name}") 
                         END AS row_count 
                         FROM pg_class 
                         WHERE relname = '{table_name}'
                           AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')) t
                    """
                    df_count = spark.read.jdbc(url=url, table=count_query, properties=properties)
                    row_count = df_count.collect()[0]["row_count"] if df_count.count() > 0 else 0
                    logging.info(f"Nombre de lignes pour {table_name}: {row_count}")

                    # Lecture des données (partitionnée si volumineuse)
                    if table_name == "sale_order_line" or (row_count and row_count > 100000):
                        logging.info(f"Partitionnement de la table {table_name} avec {row_count} lignes")
                        df_data = spark.read.jdbc(
                            url=url,
                            table=f'public."{table_name}"',
                            column="id",        # TODO: adapter si la PK n'est pas "id"
                            lowerBound=1,
                            upperBound=max(int(row_count), 1000000),
                            numPartitions=10,
                            properties=properties
                        )
                    else:
                        df_data = spark.read.jdbc(url=url, table=f'public."{table_name}"', properties=properties)

                    sample_data = df_data.limit(10).collect()

                    # Ecriture HDFS
                    try:
                        df_data.write.mode("overwrite").parquet(
                            f"hdfs://localhost:9000/datalake/raw/{source_name}/{table_name}"
                        )
                    except Exception as e:
                        logging.error(f"Échec de l'écriture en Parquet pour {table_name}: {str(e)}")
                        # On continue quand même le reporting
                        pass

                    tables_info.append({
                        "source_name": source_name,
                        "table_name": table_name,
                        "table_type": next(t["table_type"] for t in tables if t["table_name"] == table_name),
                        "columns": columns,
                        "row_count": row_count,
                        "sample_data": [row.asDict() for row in sample_data]
                    })
                    logging.info(f"Table {table_name} traitée: {len(columns)} colonnes, {row_count} lignes")
                except Exception as e:
                    logging.error(f"Erreur lors du traitement de la table {table_name}: {str(e)}")
                    failed_tables.append({"source_name": source_name, "table_name": table_name, "error": str(e)})
                    tables_info.append({"source_name": source_name, "table_name": table_name, "error": str(e)})
                    continue

    except Exception as e:
        logging.error(f"Erreur de connexion à {source_name}: {str(e)}")
        tables_info.append({"source_name": source_name, "error": str(e)})
    finally:
        if tunnel:
            try:
                tunnel.stop()
                logging.info(f"Tunnel SSH fermé pour {source_name}")
            except Exception as e:
                logging.warning(f"Erreur lors de la fermeture du tunnel pour {source_name}: {str(e)}")

    # Sauvegardes cache & erreurs
    try:
        with open(cache_path, "w") as f:
            json.dump(tables_info, f, indent=2, default=json_serial)
        logging.info(f"Cache enregistré pour {source_name}: {cache_path}")
    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement du cache pour {source_name}: {str(e)}")

    try:
        with open(failed_tables_path, "w") as f:
            json.dump(failed_tables, f, indent=2, default=json_serial)
        logging.info(f"Tables échouées enregistrées: {failed_tables_path}")
    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement des tables échouées pour {source_name}: {str(e)}")

    return tables_info


@retry(stop_max_attempt_number=3, wait_fixed=2000)
def discover_excel(source, spark):
    logging.info(f"Début de la découverte pour {source['name']} (excel)")
    path = source["path"]
    tables_info = []
    
    try:
        df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(path)
        
        columns = [{"name": col, "type": str(df.schema[col].dataType)} for col in df.columns]
        sample_data = df.limit(10).collect()
        
        row_count = df.count()
        
        tables_info.append({
            "source_name": source["name"],
            "table_name": os.path.basename(path),
            "table_type": "EXCEL",
            "columns": columns,
            "row_count": row_count,
            "sample_data": [row.asDict() for row in sample_data]
        })
        logging.info(f"Fichier Excel {path} traité: {len(columns)} colonnes, {row_count} lignes")
        
        df.write.mode("overwrite").parquet(f"hdfs://localhost:9000/datalake/raw/{source['name']}/{os.path.basename(path)}")
    
    except Exception as e:
        logging.error(f"Erreur lors du traitement de {source['name']}: {str(e)}")
        tables_info.append({"source_name": source["name"], "error": str(e)})
    
    return tables_info

def main():
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

    config_path = "/home/vagrant/datalake-mavis/provision/config/data_sources.json"
    try:
        with open(config_path) as f:
            sources = json.load(f)
    except Exception as e:
        logging.error(f"Erreur lors du chargement de la config: {str(e)}")
        spark.stop()
        return

    discovery_report = {}

    with ThreadPoolExecutor(max_workers=1) as executor:
        future_to_source = {
            executor.submit(discover_postgres, source, spark, i): source
            for i, source in enumerate(sources) if source["type"] == "postgres"
        }
        for future in as_completed(future_to_source):
            source = future_to_source[future]
            try:
                tables_info = future.result()
                if not tables_info or any("error" in info for info in tables_info):
                    error_msg = next((info.get('error', 'Erreur inconnue') for info in tables_info if "error" in info), 'Aucune donnée renvoyée')
                    logging.error(f"Échec de la découverte pour {source['name']}: {error_msg}")
                    print(f"❌ Échec de la découverte pour {source['name']} (postgres): {error_msg}")
                else:
                    logging.info(f"Découverte réussie pour {source['name']}")
                    print(f"✅ Découverte réussie pour {source['name']} (postgres)")
                discovery_report[source["name"]] = tables_info
            except Exception as e:
                logging.error(f"Erreur lors de la découverte pour {source['name']}: {str(e)}")
                print(f"❌ Erreur pour {source['name']} (postgres): {str(e)}")
                discovery_report[source["name"]] = [{"source_name": source["name"], "error": str(e)}]

    excel_sources = [source for source in sources if source["type"] == "excel"]
    with ThreadPoolExecutor(max_workers=1) as executor:
        future_to_source = {executor.submit(discover_excel, source, spark): source for source in excel_sources}
        for future in as_completed(future_to_source):
            source = future_to_source[future]
            try:
                tables_info = future.result()
                if not tables_info or any("error" in info for info in tables_info):
                    error_msg = next((info.get('error', 'Erreur inconnue') for info in tables_info if "error" in info), 'Aucune données renvoyée')
                    logging.error(f"Échec de la découverte pour {source['name']}: {error_msg}")
                    print(f"❌ Échec de la découverte pour {source['name']} (excel): {error_msg}")
                else:
                    logging.info(f"Découverte réussie pour {source['name']}")
                    print(f"✅ Découverte réussie pour {source['name']} (excel)")
                discovery_report[source["name"]] = tables_info
            except Exception as e:
                logging.error(f"Erreur lors de la découverte pour {source['name']}: {str(e)}")
                print(f"❌ Erreur pour {source['name']} (excel): {str(e)}")
                discovery_report[source["name"]] = [{"source_name": source["name"], "error": str(e)}]

    output_dir = "/home/vagrant/datalake-mavis/provision/spark-jobs/discovery"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "discovery_report.json")
    try:
        with open(output_file, "w") as f:
            json.dump(discovery_report, f, indent=2, default=json_serial)
        print(f"\n✅ Rapport de découverte généré: {output_file}")
    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement du rapport: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    main()
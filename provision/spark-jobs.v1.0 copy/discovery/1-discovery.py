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

# Configuration des logs
logging.basicConfig(
    filename="/home/vagrant/datalake-mavis/provision/logs/datalake.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialiser le modèle TALN
try:
    model = SentenceTransformer('all-MiniLM-L6-v2')
    logging.info("Modèle SentenceTransformers chargé avec succès")
except Exception as e:
    logging.error(f"Erreur lors du chargement du modèle TALN: {str(e)}")
    model = None

# Termes clés pour FHIR/RMA
FHIR_KEYWORDS = [
    "patient", "person", "individual", "diagnosis", "condition", "hospitalization",
    "encounter", "consultation", "admission", "discharge", "treatment", "procedure",
    "ward", "clinical", "evaluation", "health", "medical"
]

def json_serial(obj):
    """JSON serializer pour objets non sérialisables."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return str(obj)

def select_relevant_tables(tables, columns_data, threshold=0.80):
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

def test_ssh_connection(host, port, user, private_key=None, password=None, remote_host="localhost", remote_port=5432, timeout=10):
    """Teste la connectivité réseau, l'authentification SSH, et l'accès au port distant."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        if result != 0:
            logging.error(f"Échec de la connexion réseau à {host}:{port}, erreur code: {result}")
            return False, f"Échec de la connexion réseau: code {result}"
        logging.info(f"Connexion réseau réussie à {host}:{port}")
    except Exception as e:
        logging.error(f"Erreur lors du test de connexion réseau à {host}:{port}: {str(e)}")
        return False, f"Échec de la connexion réseau: {str(e)}"
    
    if not password and not private_key:
        logging.error(f"Aucune méthode d'authentification SSH fournie pour {host}:{port}")
        return False, "Aucune méthode d'authentification SSH fournie"
    
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if password:
            logging.info(f"Tentative d'authentification SSH avec mot de passe pour {host}:{port}")
            ssh.connect(
                host, port=port, username=user, password=password, timeout=timeout,
                allow_agent=False, look_for_keys=False,
                disabled_algorithms={'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']}
            )
            logging.info(f"Authentification SSH réussie avec mot de passe pour {host}:{port}")
        elif private_key and os.path.exists(private_key) and os.access(private_key, os.R_OK):
            logging.info(f"Tentative d'authentification SSH avec clé privée: {private_key}")
            ssh.connect(
                host, port=port, username=user, key_filename=private_key, timeout=timeout,
                allow_agent=False, look_for_keys=False,
                disabled_algorithms={'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']}
            )
            logging.info(f"Authentification SSH réussie avec clé privée pour {host}:{port}")
        else:
            logging.error(f"Clé privée SSH invalide ou non spécifiée pour {host}:{port}")
            raise ValueError("Clé privée SSH invalide ou non spécifiée")
        
        # Tester l'accès au port distant
        try:
            transport = ssh.get_transport()
            channel = transport.open_channel("direct-tcpip", (remote_host, remote_port), ("127.0.0.1", 0))
            channel.close()
            logging.info(f"Accès au port distant {remote_host}:{remote_port} via SSH réussi")
        except Exception as e:
            logging.error(f"Échec de l'accès au port distant {remote_host}:{remote_port}: {str(e)}")
            ssh.close()
            return False, f"Échec de l'accès au port distant: {str(e)}"
        
        ssh.close()
        return True, None
    except Exception as e:
        logging.error(f"Échec de l'authentification SSH pour {host}:{port}: {str(e)}")
        return False, f"Échec de l'authentification SSH: {str(e)}"

def discover_postgres(source, spark, source_index):
    """Explore une base PostgreSQL et génère les métadonnées des tables pertinentes."""
    logging.info(f"Début de la découverte pour {source['name']}")
    ssh_cfg = source.get("ssh", {})  # Configuration SSH par défaut vide
    db_cfg = source["db"]
    tables_info = []
    cache_path = f"/home/vagrant/datalake-mavis/provision/spark-jobs/metadata/{source['name']}_discovery_report.json"

    if os.path.exists(cache_path):
        logging.info(f"Cache trouvé pour {source_name}: {cache_path}")
        with open(cache_path, "r") as f:
            tables_info = json.load(f)
    
    # Créer le répertoire metadata si nécessaire
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)

    try:
        # Récupérer les identifiants
        ssh_password = os.getenv(f"SSH_PASSWORD_{source['name']}", ssh_cfg.get("password") if ssh_cfg else None)
        ssh_private_key = ssh_cfg.get("private_key")
        db_password = os.getenv(f"POSTGRES_PASSWORD_{source['name']}", db_cfg.get("password"))
        
        logging.info(f"SSH Password défini: {bool(ssh_password)}")
        logging.info(f"SSH Private Key défini: {bool(ssh_private_key)}")
        logging.info(f"DB Password défini: {bool(db_password)}")

        # Valider les identifiants
        if not db_password:
            raise ValueError(f"Mot de passe de la base de données manquant pour {source['name']}")

        # Vérifier la clé privée si fournie
        if ssh_private_key:
            if not os.path.exists(ssh_private_key):
                logging.warning(f"Clé privée SSH introuvable: {ssh_private_key}, tentative avec mot de passe")
                ssh_private_key = None
            elif not os.access(ssh_private_key, os.R_OK):
                logging.warning(f"Clé privée SSH non lisible (vérifiez les permissions): {ssh_private_key}, tentative avec mot de passe")
                ssh_private_key = None

        # Configurer le tunnel SSH si nécessaire
        tunnel = None
        if ssh_cfg and ssh_cfg.get("host") and ssh_cfg.get("port") and ssh_cfg.get("user"):
            # Tester la connexion et l'authentification SSH
            success, error_msg = test_ssh_connection(
                ssh_cfg["host"], ssh_cfg["port"], ssh_cfg["user"],
                private_key=ssh_private_key, password=ssh_password,
                remote_host=db_cfg["host"], remote_port=db_cfg["port"]
            )
            if not success:
                tables_info.append({"source_name": source["name"], "error": error_msg})
                return tables_info

            # Prioriser l'authentification par mot de passe
            try:
                if ssh_password:
                    logging.info(f"Utilisation du mot de passe SSH pour {source['name']}")
                    tunnel = SSHTunnelForwarder(
                        ssh_address_or_host=(ssh_cfg["host"], ssh_cfg["port"]),
                        ssh_username=ssh_cfg["user"],
                        ssh_password=ssh_password,
                        remote_bind_address=(db_cfg["host"], db_cfg["port"]),
                        local_bind_address=('127.0.0.1', 15432 + source_index),
                        set_keepalive=30
                    )
                    tunnel.start()
                    logging.info(f"Tunnel SSH établi avec mot de passe sur port local {tunnel.local_bind_port}")
                    url = f"jdbc:postgresql://127.0.0.1:{tunnel.local_bind_port}/{db_cfg['database']}?socketTimeout=120&connectTimeout=30"
                elif ssh_private_key:
                    logging.info(f"Utilisation de la clé privée SSH: {ssh_private_key}")
                    tunnel = SSHTunnelForwarder(
                        ssh_address_or_host=(ssh_cfg["host"], ssh_cfg["port"]),
                        ssh_username=ssh_cfg["user"],
                        ssh_private_key=ssh_private_key,
                        remote_bind_address=(db_cfg["host"], db_cfg["port"]),
                        local_bind_address=('127.0.0.1', 15432 + source_index),
                        set_keepalive=30,
                    )
                    tunnel.start()
                    logging.info(f"Tunnel SSH établi avec clé privée sur port local {tunnel.local_bind_port}")
                    url = f"jdbc:postgresql://127.0.0.1:{tunnel.local_bind_port}/{db_cfg['database']}?socketTimeout=120&connectTimeout=30"
                else:
                    raise ValueError(f"Aucune méthode d'authentification SSH valide pour {source['name']}")
            except Exception as e:
                logging.error(f"Erreur lors de l'établissement du tunnel SSH pour {source['name']}: {str(e)}")
                tables_info.append({"source_name": source["name"], "error": f"Échec du tunnel SSH: {str(e)}"})
                return tables_info
        else:
            url = f"jdbc:postgresql://{db_cfg['host']}:{db_cfg['port']}/{db_cfg['database']}?socketTimeout=120&connectTimeout=30"
            logging.info(f"Connexion directe sans SSH pour {source['name']}")

        # Propriétés de connexion JDBC
        properties = {
            "user": db_cfg["user"],
            "password": db_password,
            "driver": "org.postgresql.Driver",
            "ssl": "false",
            "connectTimeout": "30",
            "socketTimeout": "120"
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

                    # Éviter count() pour les grandes tables
                    row_count = None
                    try:
                        row_count = df_data.count()
                    except Exception as e:
                        logging.warning(f"Échec du comptage des lignes pour {table_name}: {str(e)}")
                        row_count = -1  # Indiquer une erreur sans interrompre

                    # Enregistrer les données brutes en Parquet
                    try:
                        df_data.write.mode("overwrite").parquet(f"hdfs://localhost:9000/datalake/raw/{source['name']}/{table_name}")
                    except Exception as e:
                        logging.error(f"Échec de l'écriture en Parquet pour {table_name}: {str(e)}")
                        continue

                    # Ajouter les métadonnées
                    tables_info.append({
                        "source_name": source["name"],
                        "table_name": table_name,
                        "table_type": next(t["table_type"] for t in tables if t["table_name"] == table_name),
                        "columns": columns,
                        "row_count": row_count,
                        "sample_data": [row.asDict() for row in sample_data]
                    })
                    logging.info(f"Table {table_name} traitée: {len(columns)} colonnes, {row_count} lignes")
                except Exception as e:
                    logging.error(f"Erreur lors du traitement de la table {table_name}: {str(e)}")
                    tables_info.append({"source_name": source["name"], "table_name": table_name, "error": str(e)})
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
        
        tables_info.append({
            "source_name": source["name"],
            "table_name": os.path.basename(path),
            "table_type": "EXCEL",
            "columns": columns,
            "row_count": df.count(),
            "sample_data": [row.asDict() for row in sample_data]
        })
        logging.info(f"Fichier Excel {path} traité: {len(columns)} colonnes, {df.count()} lignes")
        
        # Enregistrer les données brutes en Parquet
        df.write.mode("overwrite").parquet(f"hdfs://localhost:9000/datalake/raw/{source['name']}/{os.path.basename(path)}")
    
    except Exception as e:
        logging.error(f"Erreur lors du traitement de {source['name']}: {str(e)}")
        tables_info.append({"source_name": source["name"], "error": str(e)})
    
    return tables_info

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
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_source = {
            executor.submit(discover_postgres, source, spark, i): source
            for i, source in enumerate(sources) if source["type"] == "postgres"
        }
        for future in as_completed(future_to_source):
            source = future_to_source[future]
            try:
                tables_info = future.result()
                # Vérifier si tables_info est vide ou contient une erreur
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

    # Traiter les sources Excel
    for source in sources:
        if source["type"] == "excel":
            try:
                tables_info = discover_excel(source, spark)
                if not tables_info or any("error" in info for info in tables_info):
                    error_msg = next((info.get('error', 'Erreur inconnue') for info in tables_info if "error" in info), 'Aucune donnée renvoyée')
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
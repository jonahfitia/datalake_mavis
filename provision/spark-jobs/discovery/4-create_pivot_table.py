from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import col, coalesce, broadcast, dense_rank
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from sentence_transformers import SentenceTransformer, util
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid
from pyspark.sql import Window
import logging
import types

# Configuration des logs
logging.basicConfig(
    filename="/home/vagrant/datalake-mavis/provision/logs/pivot_table.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Vérification que col est bien une fonction
if not isinstance(col, types.FunctionType):
    logging.error("Erreur : 'col' n'est pas une fonction. Vérifiez les importations ou les redéfinitions.")
    raise ImportError("La fonction 'col' de pyspark.sql.functions n'est pas correctement définie.")

# Définir le schéma pivot
pivot_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("birth_date", TimestampType(), True),
    StructField("date", TimestampType(), True),
    StructField("appointment_date", TimestampType(), True),
    StructField("admission_date", TimestampType(), True),
    StructField("weight", DoubleType(), True),
    StructField("height", DoubleType(), True),
    StructField("bmi", DoubleType(), True),
    StructField("birth_weight", DoubleType(), True),
    StructField("birth_height", DoubleType(), True),
    StructField("hb", DoubleType(), True),
    StructField("hiv", StringType(), True),
    StructField("ward_name", StringType(), True),
    StructField("diagnosis", StringType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("gravida", IntegerType(), True),
    StructField("abortions", IntegerType(), True),
    StructField("stillbirths", IntegerType(), True)
])

# Initialiser Spark
spark = SparkSession.builder \
    .appName("PivotTableCreation") \
    .master("local[*]") \
    .config("spark.jars", "/home/vagrant/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Charger fhir_mapping.json
try:
    with open("/home/vagrant/datalake-mavis/provision/spark-jobs/discovery/fhir_mapping.json") as f:
        fhir_mapping = json.load(f)
    logging.info("fhir_mapping.json chargé avec succès")
except Exception as e:
    logging.error(f"Erreur lors du chargement de fhir_mapping.json: {str(e)}")
    spark.stop()
    exit(1)

# UDF pour générer UUID
@udf(returnType=StringType())
def generate_uuid():
    return str(uuid.uuid4())

# CIM10 mapping
try:
    cim_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("sep", ";") \
        .option("quote", '"') \
        .option("encoding", "UTF-8") \
        .load("/home/vagrant/datalake-mavis/provision/config/cim10_liste.csv")
    logging.info(f"Colonnes du fichier CIM-10: {cim_df.columns}")

    # Validation des colonnes
    description_col = None
    code_col = None
    for c in cim_df.columns:
        if "libelle" in c.lower() or "lib_long" in c.lower():
            description_col = c
        if "cim10" in c.lower():
            code_col = c

    if not description_col or not code_col:
        logging.error(f"Colonnes CIM-10 manquantes. Trouvé: {cim_df.columns}")
        spark.stop()
        exit(1)

    cim_descriptions = cim_df.filter(cim_df[description_col].isNotNull()).select(description_col).rdd.flatMap(lambda x: x).collect()
    cim_codes = cim_df.select(code_col).rdd.flatMap(lambda x: x).collect()

    model = SentenceTransformer('all-MiniLM-L6-v2')
    cim_embeddings = model.encode(cim_descriptions)

    @udf(returnType=StringType())
    def map_to_cim10(diagnosis):
        if diagnosis is None:
            return None
        embedding = model.encode(diagnosis)
        similarities = util.cos_sim(embedding, cim_embeddings)[0]
        max_idx = similarities.argmax().item()
        if similarities[max_idx] > 0.7:
            return cim_codes[max_idx]
        return "UNKNOWN"

except Exception as e:
    logging.error(f"Erreur lors du chargement du fichier CIM-10 ou du modèle TALN: {str(e)}")
    spark.stop()
    exit(1)

# Charger les fichiers Parquet depuis HDFS
dfs = {}
for source_name in fhir_mapping:
    dfs[source_name] = {}
    for table_name in fhir_mapping[source_name]:
        parquet_path = f"hdfs://localhost:9000/datalake/raw/{source_name}/{table_name}"
        try:
            dfs[source_name][table_name] = spark.read.parquet(parquet_path)
            logging.info(f"Chargé {parquet_path} avec colonnes: {dfs[source_name][table_name].columns}")
        except Exception as e:
            logging.error(f"Erreur lors de la lecture de {parquet_path}: {str(e)}")
            continue

# Construire la table pivot
pivot_data = None
existing_columns = set()
patient_table = None
for source_name in dfs:
    for table_name, df in dfs[source_name].items():
        if "patient" in table_name.lower():
            patient_table = df
            logging.info(f"Tableau des patients identifié: {source_name}.{table_name}")
        mapped_cols = []
        patient_id_col = None
        mapped_fields = set()
        diagnosis_mapped = False  # Pour limiter à une seule colonne diagnosis par table
        logging.info(f"Colonnes brutes pour {source_name}.{table_name}: {df.columns}")
        for col_name in df.columns:
            col_name_lower = col_name.lower()
            if col_name_lower in fhir_mapping[source_name][table_name]:
                fhir_field = fhir_mapping[source_name][table_name][col_name_lower]["fhir_field"]
                fhir_resource = fhir_mapping[source_name][table_name][col_name_lower].get("fhir_resource")
                code = fhir_mapping[source_name][table_name][col_name_lower].get("code")
                field_key = f"{fhir_field}_{fhir_resource}_{code if code else ''}_{col_name}"
                if field_key in mapped_fields:
                    logging.warning(f"Colonne {col_name} ignorée dans {source_name}.{table_name} car déjà mappée")
                    continue
                # Logique pour patient_id
                if patient_id_col is None:
                    if col_name_lower in ["patient", "patient_id"] and fhir_field == "id" and fhir_resource == "Patient":
                        patient_id_col = col(col_name).cast("string").alias("patient_id")
                        mapped_cols.append(patient_id_col)
                        mapped_fields.add("patient_id")
                        logging.info(f"Ajout de patient_id depuis FK {col_name} dans {source_name}.{table_name}")
                    elif "patient" in table_name.lower() and col_name_lower == "id" and fhir_field == "id" and fhir_resource == "Patient":
                        patient_id_col = col(col_name).cast("string").alias("patient_id")
                        mapped_cols.append(patient_id_col)
                        mapped_fields.add("patient_id")
                        logging.info(f"Ajout de patient_id depuis ID principal dans {source_name}.{table_name}")
                # Fallback UUID
                if patient_id_col is None and fhir_field == "id":
                    patient_id_col = generate_uuid().alias("patient_id")
                    mapped_cols.append(patient_id_col)
                    mapped_fields.add("patient_id")
                    logging.info(f"Génération UUID pour patient_id dans {source_name}.{table_name}")
                # Autres mappings avec noms temporaires
                temp_col_name = f"{col_name}_{source_name}_{table_name}"
                if fhir_field == "name.given" and "name" not in existing_columns:
                    mapped_cols.append(col(col_name).cast("string").alias(temp_col_name))
                    mapped_fields.add(f"name_{fhir_resource}")
                    logging.info(f"Ajout de name depuis {col_name} (temp: {temp_col_name})")
                elif fhir_field == "birthDate" and "birth_date" not in existing_columns:
                    mapped_cols.append(col(col_name).cast("timestamp").alias(temp_col_name))
                    mapped_fields.add(f"birth_date_{fhir_resource}")
                    logging.info(f"Ajout de birth_date depuis {col_name} (temp: {temp_col_name})")
                elif fhir_field == "effectiveDateTime" and "date" not in existing_columns:
                    if col_name_lower in ["create_date", "date"]:
                        mapped_cols.append(col(col_name).cast("timestamp").alias(temp_col_name))
                        mapped_fields.add(f"date_{fhir_resource}")
                        logging.info(f"Ajout de date depuis {col_name} (temp: {temp_col_name})")
                elif fhir_field == "code" and fhir_resource == "Condition" and "diagnosis" not in existing_columns and not diagnosis_mapped:
                    if col_name_lower in ["info_diagnosis", "diagnosis"]:  # Prioriser info_diagnosis
                        mapped_cols.append(col(col_name).cast("string").alias(temp_col_name))
                        mapped_cols.append(map_to_cim10(col(col_name)).alias(f"diagnosis_code_{col_name}_{source_name}_{table_name}"))
                        mapped_fields.add(f"diagnosis_{fhir_resource}")
                        diagnosis_mapped = True
                        logging.info(f"Ajout de diagnosis et diagnosis_code depuis {col_name} (temp: {temp_col_name}, diagnosis_code: diagnosis_code_{col_name}_{source_name}_{table_name})")
                elif fhir_field == "name" and fhir_resource == "Location" and "ward_name" not in existing_columns:
                    if col_name_lower in ["department_id", "ward_name"]:
                        mapped_cols.append(col(col_name).cast("string").alias(temp_col_name))
                        mapped_fields.add(f"ward_name_{fhir_resource}")
                        logging.info(f"Ajout de ward_name depuis {col_name} (temp: {temp_col_name})")
                elif fhir_field == "period.start" and fhir_resource == "Encounter" and "admission_date" not in existing_columns:
                    if any(x in table_name.lower() for x in ["inpatient", "hospitalization", "perinatal"]) and col_name_lower in ["admission_date", "date_from", "hospitalization_date"]:
                        mapped_cols.append(col(col_name).cast("timestamp").alias(temp_col_name))
                        mapped_fields.add(f"admission_date_{fhir_resource}")
                        logging.info(f"Ajout de admission_date depuis {col_name} (temp: {temp_col_name})")
                elif fhir_field == "period.start" and fhir_resource == "Encounter" and "appointment_date" not in existing_columns:
                    if "appointment" in table_name.lower() and col_name_lower in ["appointment_date", "date"]:
                        mapped_cols.append(col(col_name).cast("timestamp").alias(temp_col_name))
                        mapped_fields.add(f"appointment_date_{fhir_resource}")
                        logging.info(f"Ajout de appointment_date depuis {col_name} (temp: {temp_col_name})")
                elif fhir_field == "valueQuantity":
                    if code == "29463-7" and "weight" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("double").alias(temp_col_name))
                        mapped_fields.add(f"weight_{code}")
                        logging.info(f"Ajout de weight depuis {col_name} (temp: {temp_col_name})")
                    elif code == "8302-2" and "height" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("double").alias(temp_col_name))
                        mapped_fields.add(f"height_{code}")
                        logging.info(f"Ajout de height depuis {col_name} (temp: {temp_col_name})")
                    elif code == "8339-4" and "birth_weight" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("double").alias(temp_col_name))
                        mapped_fields.add(f"birth_weight_{code}")
                        logging.info(f"Ajout de birth_weight depuis {col_name} (temp: {temp_col_name})")
                    elif code == "39156-5" and "bmi" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("double").alias(temp_col_name))
                        mapped_fields.add(f"bmi_{code}")
                        logging.info(f"Ajout de bmi depuis {col_name} (temp: {temp_col_name})")
                    elif code == "718-7" and "hb" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("double").alias(temp_col_name))
                        mapped_fields.add(f"hb_{code}")
                        logging.info(f"Ajout de hb depuis {col_name} (temp: {temp_col_name})")
                    elif code == "11612-9":
                        if col_name_lower in ["gravida", "gravida_number"] and "gravida" not in existing_columns:
                            mapped_cols.append(col(col_name).cast("integer").alias(temp_col_name))
                            mapped_fields.add(f"gravida_{code}")
                            logging.info(f"Ajout de gravida depuis {col_name} (temp: {temp_col_name})")
                        elif col_name_lower == "abortions" and "abortions" not in existing_columns:
                            mapped_cols.append(col(col_name).cast("integer").alias(temp_col_name))
                            mapped_fields.add(f"abortions_{code}")
                            logging.info(f"Ajout de abortions depuis {col_name} (temp: {temp_col_name})")
                        elif col_name_lower == "stillbirths" and "stillbirths" not in existing_columns:
                            mapped_cols.append(col(col_name).cast("integer").alias(temp_col_name))
                            mapped_fields.add(f"stillbirths_{code}")
                            logging.info(f"Ajout de stillbirths depuis {col_name} (temp: {temp_col_name})")
                elif fhir_field == "valueCodeableConcept":
                    if code == "55277-8" and "hiv" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("string").alias(temp_col_name))
                        mapped_fields.add(f"hiv_{code}")
                        logging.info(f"Ajout de hiv depuis {col_name} (temp: {temp_col_name})")
        if mapped_cols:
            df_mapped = df.select(mapped_cols)
            logging.info(f"Colonnes mappées pour {source_name}.{table_name}: {[c._jc.toString() for c in mapped_cols]}")
            # Vérifier si une colonne avec l'alias patient_id existe
            has_patient_id = any(c._jc.toString().endswith('AS `patient_id`') for c in mapped_cols)
            if not has_patient_id:
                logging.warning(f"patient_id absent dans les colonnes mappées pour {source_name}.{table_name}")
            elif pivot_data is None:
                # Initialisation dynamique avec la première table contenant patient_id
                if "patient" in table_name.lower() or patient_id_col is not None:
                    pivot_data = df_mapped
                    pivot_data_initialized = True
                    logging.info(f"Initialisation de pivot_data avec {source_name}.{table_name}: {df_mapped.columns}")
                else:
                    logging.warning(f"Ignorer {source_name}.{table_name} pour l'initialisation de pivot_data")
            else:
                pivot_data = pivot_data.join(broadcast(df_mapped), "patient_id", "left")
                logging.info(f"Colonnes après jointure avec {source_name}.{table_name}: {pivot_data.columns}")
        
        # Nettoyage uniquement si pivot_data est défini
        if pivot_data is not None:
            for col_name in pivot_schema.names:
                if col_name != "patient_id" and col_name != "diagnosis_code":
                    col_variants = [c for c in pivot_data.columns if c.startswith(f"{col_name}_") or c == col_name]
                    if len(col_variants) > 1:
                        pivot_data = pivot_data.withColumn(col_name, coalesce(*[col(c) for c in col_variants]))
                        pivot_data = pivot_data.drop(*[c for c in col_variants if c != col_name])
                        logging.info(f"Nettoyage immédiat de {len(col_variants)} colonnes pour {col_name}: {col_variants}")
                    elif len(col_variants) == 1 and col_variants[0] != col_name:
                        pivot_data = pivot_data.withColumnRenamed(col_variants[0], col_name)
                        logging.info(f"Renommage de {col_variants[0]} en {col_name}")
            # Gérer diagnosis_code séparément avec unicité
            diagnosis_code_variants = [c for c in pivot_data.columns if c.startswith("diagnosis_code_")]
            if len(diagnosis_code_variants) > 1:
                pivot_data = pivot_data.withColumn("diagnosis_code", coalesce(*[col(c) for c in diagnosis_code_variants]))
                pivot_data = pivot_data.drop(*diagnosis_code_variants)
                logging.info(f"Nettoyage de {len(diagnosis_code_variants)} colonnes pour diagnosis_code: {diagnosis_code_variants}")
            elif len(diagnosis_code_variants) == 1 and diagnosis_code_variants[0] != "diagnosis_code":
                pivot_data = pivot_data.withColumnRenamed(diagnosis_code_variants[0], "diagnosis_code")
                logging.info(f"Renommage de {diagnosis_code_variants[0]} en diagnosis_code")
            existing_columns.update([c for c in pivot_data.columns if c != "patient_id"])
            logging.info(f"Colonnes après nettoyage pour {source_name}.{table_name}: {pivot_data.columns}")
        else:
            logging.warning(f"pivot_data non initialisé après traitement de {source_name}.{table_name}")
            
# Déduplication si pivot_data existe
if pivot_data:
    # Déduplication par name et birth_date
    window_spec = Window.partitionBy("name", "birth_date").orderBy("patient_id")
    pivot_data = pivot_data.withColumn("rank", dense_rank().over(window_spec))
    pivot_data = pivot_data.filter(col("rank") == 1).drop("rank")
    logging.info("Déduplication effectuée sur name et birth_date")
    # Assigner UUID si doublons détectés
    duplicate_count = pivot_data.groupBy("name", "birth_date").count().filter(col("count") > 1).count()
    if duplicate_count > 0:
        pivot_data = pivot_data.withColumn("patient_id", generate_uuid())
        logging.info(f"Doublons détectés ({duplicate_count}), UUID assignés")

    # Appliquer le schéma pivot
    pivot_data = pivot_data.select([col(c.name).cast(c.dataType).alias(c.name) for c in pivot_schema.fields if c.name in pivot_data.columns])
    logging.info(f"Colonnes finales: {pivot_data.columns}")
    try:
        pivot_data.write.mode("overwrite").parquet("hdfs://localhost:9000/datalake/pivot/pivot_table")
        logging.info("Table pivot enregistrée dans hdfs://localhost:9000/datalake/pivot/pivot_table")
    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement de la table pivot: {str(e)}")
        spark.stop()
        exit(1)
else:
    logging.warning("Aucune donnée pour la table pivot")

spark.stop()
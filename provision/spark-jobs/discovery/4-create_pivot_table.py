from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import col, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

# Définir le schéma pivot
pivot_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("name", StringType(), True),
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
with open("/home/vagrant/datalake-mavis/provision/config/fhir_mapping.json") as f:
    fhir_mapping = json.load(f)

# Charger les fichiers Parquet depuis HDFS
dfs = {}
for source_name in fhir_mapping:
    dfs[source_name] = {}
    for table_name in fhir_mapping[source_name]:
        parquet_path = f"hdfs://localhost:9000/datalake/raw/{source_name}/{table_name}"
        try:
            dfs[source_name][table_name] = spark.read.parquet(parquet_path)
        except Exception as e:
            print(f"Erreur lors de la lecture de {parquet_path}: {str(e)}")
            continue

# Construire la table pivot
pivot_data = None
existing_columns = set()  # Suivre les colonnes non-clés déjà présentes dans pivot_data
for source_name in dfs:
    for table_name, df in dfs[source_name].items():
        mapped_cols = []
        patient_id_col = None
        name_col = None
        date_col = None
        diagnosis_col = None
        ward_name_col = None
        admission_date_col = None
        appointment_date_col = None
        gravida_col = None
        abortions_col = None
        stillbirths_col = None
        # Dictionnaire pour suivre les colonnes déjà mappées dans cette table
        mapped_fields = set()
        # Debug: Afficher les colonnes brutes de la table
        print(f"Colonnes brutes pour {source_name}.{table_name}: {df.columns}")
        for col_name in df.columns:
            col_name_lower = col_name.lower()
            if col_name_lower in fhir_mapping[source_name][table_name]:
                fhir_field = fhir_mapping[source_name][table_name][col_name_lower]["fhir_field"]
                fhir_resource = fhir_mapping[source_name][table_name][col_name_lower].get("fhir_resource")
                code = fhir_mapping[source_name][table_name][col_name_lower].get("code")
                # Clé unique pour éviter les duplications dans une même table
                field_key = f"{fhir_field}_{fhir_resource}_{code if code else ''}"
                if field_key in mapped_fields:
                    print(f"Attention: Colonne {col_name} ignorée dans {source_name}.{table_name} car déjà mappée pour {fhir_field} ({fhir_resource})")
                    continue
                # Inclure patient_id pour la jointure, mais une seule fois
                if fhir_field == "id" and patient_id_col is None:
                    if col_name_lower in ["id", "patient_id", "partner_id"]:
                        patient_id_col = col(col_name).cast("string").alias("patient_id")
                        mapped_cols.append(patient_id_col)
                        mapped_fields.add(field_key)
                        print(f"Ajout de patient_id depuis {source_name}.{table_name} (colonne source: {col_name})")
                # Autres colonnes : vérifier si elles existent déjà dans pivot_data
                elif fhir_field == "name.given" and name_col is None:
                    if table_name in ["hms_patient", "gnuhealth_patient"] and "name" not in existing_columns:
                        name_col = col(col_name).cast("string").alias("name")
                        mapped_cols.append(name_col)
                        mapped_fields.add(field_key)
                        print(f"Ajout de name depuis {source_name}.{table_name} (colonne source: {col_name})")
                elif fhir_field == "effectiveDateTime" and date_col is None:
                    if col_name_lower in ["create_date", "date"] and "date" not in existing_columns:
                        date_col = col(col_name).cast("timestamp").alias("date")
                        mapped_cols.append(date_col)
                        mapped_fields.add(field_key)
                        print(f"Ajout de date depuis {source_name}.{table_name} (colonne source: {col_name})")
                elif fhir_field == "code" and fhir_resource == "Condition" and diagnosis_col is None:
                    if col_name_lower in ["info_diagnosis", "diagnosis"] and "diagnosis" not in existing_columns:
                        diagnosis_col = col(col_name).cast("string").alias("diagnosis")
                        mapped_cols.append(diagnosis_col)
                        mapped_fields.add(field_key)
                        print(f"Ajout de diagnosis depuis {source_name}.{table_name} (colonne source: {col_name})")
                elif fhir_field == "name" and fhir_resource == "Location" and ward_name_col is None:
                    if col_name_lower in ["department_id", "ward_name"] and "ward_name" not in existing_columns:
                        ward_name_col = col(col_name).cast("string").alias("ward_name")
                        mapped_cols.append(ward_name_col)
                        mapped_fields.add(field_key)
                        print(f"Ajout de ward_name depuis {source_name}.{table_name} (colonne source: {col_name})")
                elif fhir_field == "period.start" and fhir_resource == "Encounter" and admission_date_col is None:
                    if any(x in table_name.lower() for x in ["inpatient", "hospitalization", "perinatal"]) and col_name_lower in ["admission_date", "date_from"] and "admission_date" not in existing_columns:
                        admission_date_col = col(col_name).cast("timestamp").alias("admission_date")
                        mapped_cols.append(admission_date_col)
                        mapped_fields.add(field_key)
                        print(f"Ajout de admission_date depuis {source_name}.{table_name} (colonne source: {col_name})")
                elif fhir_field == "period.start" and fhir_resource == "Encounter" and appointment_date_col is None:
                    if "appointment" in table_name.lower() and col_name_lower in ["appointment_date", "date"] and "appointment_date" not in existing_columns:
                        appointment_date_col = col(col_name).cast("timestamp").alias("appointment_date")
                        mapped_cols.append(appointment_date_col)
                        mapped_fields.add(field_key)
                        print(f"Ajout de appointment_date depuis {source_name}.{table_name} (colonne source: {col_name})")
                elif fhir_field == "valueQuantity":
                    if code == "29463-7" and "weight" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("double").alias("weight"))
                        mapped_fields.add(field_key)
                        print(f"Ajout de weight depuis {source_name}.{table_name} (colonne source: {col_name})")
                    elif code == "8302-2" and "height" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("double").alias("height"))
                        mapped_fields.add(field_key)
                        print(f"Ajout de height depuis {source_name}.{table_name} (colonne source: {col_name})")
                    elif code == "8339-4" and "birth_weight" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("double").alias("birth_weight"))
                        mapped_fields.add(field_key)
                        print(f"Ajout de birth_weight depuis {source_name}.{table_name} (colonne source: {col_name})")
                    elif code == "39156-5" and "bmi" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("double").alias("bmi"))
                        mapped_fields.add(field_key)
                        print(f"Ajout de bmi depuis {source_name}.{table_name} (colonne source: {col_name})")
                    elif code == "718-7" and "hb" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("double").alias("hb"))
                        mapped_fields.add(field_key)
                        print(f"Ajout de hb depuis {source_name}.{table_name} (colonne source: {col_name})")
                    elif code == "11612-9":
                        if col_name_lower in ["gravida", "gravida_number"] and gravida_col is None and "gravida" not in existing_columns:
                            gravida_col = col(col_name).cast("integer").alias("gravida")
                            mapped_cols.append(gravida_col)
                            mapped_fields.add(field_key)
                            print(f"Ajout de gravida depuis {source_name}.{table_name} (colonne source: {col_name})")
                        elif col_name_lower == "abortions" and abortions_col is None and "abortions" not in existing_columns:
                            abortions_col = col(col_name).cast("integer").alias("abortions")
                            mapped_cols.append(abortions_col)
                            mapped_fields.add(field_key)
                            print(f"Ajout de abortions depuis {source_name}.{table_name} (colonne source: {col_name})")
                        elif col_name_lower == "stillbirths" and stillbirths_col is None and "stillbirths" not in existing_columns:
                            stillbirths_col = col(col_name).cast("integer").alias("stillbirths")
                            mapped_cols.append(stillbirths_col)
                            mapped_fields.add(field_key)
                            print(f"Ajout de stillbirths depuis {source_name}.{table_name} (colonne source: {col_name})")
                elif fhir_field == "valueCodeableConcept":
                    if code == "55277-8" and "hiv" not in existing_columns:
                        mapped_cols.append(col(col_name).cast("string").alias("hiv"))
                        mapped_fields.add(field_key)
                        print(f"Ajout de hiv depuis {source_name}.{table_name} (colonne source: {col_name})")
        if mapped_cols:
            # Debug: Afficher mapped_cols avant vérification
            print(f"Colonnes dans mapped_cols pour {source_name}.{table_name} avant vérification: {[c._jc.toString() for c in mapped_cols]}")
            # Vérifier que patient_id est inclus et unique
            patient_id_cols = [c for c in mapped_cols if "AS patient_id" in c._jc.toString()]
            if len(patient_id_cols) > 1:
                print(f"Avertissement: Plusieurs colonnes patient_id détectées dans {source_name}.{table_name}: {[c._jc.toString() for c in patient_id_cols]}")
                # Garder la première occurrence de patient_id
                mapped_cols = [c for c in mapped_cols if "AS patient_id" not in c._jc.toString()] + [patient_id_cols[0]]
                print(f"Correction: Une seule colonne patient_id conservée pour {source_name}.{table_name}")
            elif len(patient_id_cols) == 0:
                # Ajouter patient_id si absent et disponible dans la table
                for col_name in df.columns:
                    col_name_lower = col_name.lower()
                    if col_name_lower in ["id", "patient_id", "partner_id"] and col_name_lower in fhir_mapping[source_name][table_name] and fhir_mapping[source_name][table_name][col_name_lower]["fhir_field"] == "id":
                        mapped_cols.append(col(col_name).cast("string").alias("patient_id"))
                        print(f"Ajout de patient_id depuis {source_name}.{table_name} (colonne source: {col_name}) pour la jointure")
                        break
            # Debug: Afficher mapped_cols après vérification
            print(f"Colonnes dans mapped_cols pour {source_name}.{table_name} après vérification: {[c._jc.toString() for c in mapped_cols]}")
            # Vérifier à nouveau si patient_id est présent
            if not any("AS patient_id" in c._jc.toString() for c in mapped_cols):
                print(f"Erreur: Aucune colonne patient_id dans {source_name}.{table_name}, jointure impossible")
                continue
            df_mapped = df.select(mapped_cols)
            print(f"Colonnes pour {source_name}.{table_name}: {df_mapped.columns}")
            if pivot_data is None:
                pivot_data = df_mapped
                existing_columns.update([c for c in df_mapped.columns if c != "patient_id"])  # Ne pas bloquer patient_id
            else:
                # Fusionner avec pivot_data
                pivot_data = pivot_data.join(df_mapped, "patient_id", "outer")
                print(f"Colonnes après jointure avec {source_name}.{table_name}: {pivot_data.columns}")
                # Nettoyer les colonnes immédiatement après la jointure
                for col_name in ["patient_id", "name", "date", "diagnosis", "ward_name", "admission_date", "appointment_date", "gravida", "abortions", "stillbirths", "weight", "height", "bmi", "birth_weight", "hb", "hiv"]:
                    col_variants = [c for c in pivot_data.columns if c.startswith(f"{col_name}_") or c == col_name]
                    if len(col_variants) > 1:
                        print(f"Nettoyage de {len(col_variants)} colonnes pour {col_name}: {col_variants}")
                        pivot_data = pivot_data.withColumn(col_name, coalesce(*[col(c) for c in col_variants]))
                        pivot_data = pivot_data.drop(*[c for c in col_variants if c != col_name])
                print(f"Colonnes après nettoyage pour {source_name}.{table_name}: {pivot_data.columns}")
                existing_columns.update([c for c in pivot_data.columns if c != "patient_id"])  # Ne pas bloquer patient_id
                # Vérifier s'il reste des doublons
                for col_name in pivot_data.columns:
                    if pivot_data.columns.count(col_name) > 1:
                        print(f"Erreur: Colonne {col_name} dupliquée après nettoyage pour {source_name}.{table_name}: {pivot_data.columns}")
        else:
            print(f"Aucune colonne mappée pour {source_name}.{table_name}, table ignorée")

# Sauvegarder la table pivot
if pivot_data:
    print(f"Colonnes finales avant select: {pivot_data.columns}")
    # Appliquer le schéma pivot
    pivot_data = pivot_data.select([col(c.name).cast(c.dataType).alias(c.name) for c in pivot_schema if c.name in pivot_data.columns])
    print(f"Colonnes finales après select: {pivot_data.columns}")
    pivot_data.write.mode("overwrite").parquet("hdfs://localhost:9000/datalake/pivot/pivot_table")
    print("Table pivot enregistrée dans hdfs://localhost:9000/datalake/pivot/pivot_table")
else:
    print("Aucune donnée pour la table pivot")

spark.stop()

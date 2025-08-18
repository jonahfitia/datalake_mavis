import json
from sentence_transformers import SentenceTransformer, util

try:
    with open("/home/vagrant/datalake-mavis/provision/spark-jobs/discovery/discovery_report.json") as f:
        report = json.load(f)
except Exception as e:
    print(f"Erreur lors du chargement du rapport: {str(e)}")
    exit(1)

# Modèle TALN pour mapping dynamique
model = SentenceTransformer('all-MiniLM-L6-v2')
fhir_fields = ["id", "name.given", "valueQuantity", "effectiveDateTime", "code", "name", "valueCodeableConcept", "birthDate", "gender"]
fhir_embeddings = model.encode(fhir_fields)

fhir_mapping = {}
for source_name in report:
    fhir_mapping[source_name] = {}
    for entry in report[source_name]:
        if "error" in entry:
            continue
        table_name = entry["table_name"]
        fhir_mapping[source_name][table_name] = {}
        for col in entry["columns"]:
            col_name = col["name"].lower()
            col_type = col["type"].lower()
            # Mapping dynamique via TALN
            col_embedding = model.encode(col_name)
            similarities = util.cos_sim(col_embedding, fhir_embeddings)[0]
            max_idx = similarities.argmax().item()
            if similarities[max_idx] > 0.7:  # Seuil pour match
                fhir_field = fhir_fields[max_idx]
            else:
                continue  # Pas de match, skip

            # Rules spécifiques pour refinement
            if fhir_field == "id":
                if col_name in ["patient", "patient_id"]:  # Prioriser FK
                    fhir_mapping[source_name][table_name][col_name] = {
                        "fhir_resource": "Patient",
                        "fhir_field": "id"
                    }
                elif col_name == "id" and "patient" in table_name.lower():
                    fhir_mapping[source_name][table_name][col_name] = {
                        "fhir_resource": "Patient",
                        "fhir_field": "id"
                    }
            elif fhir_field == "name.given" and "string" in col_type:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Patient",
                    "fhir_field": "name.given"
                }
            elif fhir_field == "valueQuantity" and "numeric" in col_type or "double" in col_type:
                codes = {
                    "weight": "29463-7",
                    "height": "8302-2",
                    "bmi": "39156-5",
                    "birth_weight": "8339-4",
                    "birth_height": "8302-2",
                    "birth_head_circum": "8287-5",
                    "screatinine": "2160-0",
                    "hb": "718-7",
                    "rbs": "1558-6",
                    "gravida": "11612-9",
                    "abortions": "11612-9",
                    "stillbirths": "11612-9"
                }
                for key, code in codes.items():
                    if key in col_name:
                        fhir_mapping[source_name][table_name][col_name] = {
                            "fhir_resource": "Observation",
                            "fhir_field": "valueQuantity",
                            "code": code
                        }
                        break
            elif fhir_field == "effectiveDateTime" and "timestamp" in col_type:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Observation" if "evaluation" in table_name.lower() else "Encounter",
                    "fhir_field": "effectiveDateTime" if "evaluation" in table_name.lower() else "period.start"
                }
            elif fhir_field == "code" and "string" in col_type:
                if "diagnosis" in col_name or "disease" in col_name:
                    fhir_mapping[source_name][table_name][col_name] = {
                        "fhir_resource": "Condition",
                        "fhir_field": "code"
                    }
            elif fhir_field == "name" and "string" in col_type:
                if "ward" in col_name or "department" in col_name:
                    fhir_mapping[source_name][table_name][col_name] = {
                        "fhir_resource": "Location",
                        "fhir_field": "name"
                    }
            elif fhir_field == "valueCodeableConcept" and "string" in col_type:
                codes = {"hiv": "55277-8", "hbsag": "5196-1"}
                for key, code in codes.items():
                    if key in col_name:
                        fhir_mapping[source_name][table_name][col_name] = {
                            "fhir_resource": "Observation",
                            "fhir_field": "valueCodeableConcept",
                            "code": code
                        }
                        break
            elif fhir_field == "birthDate" and "timestamp" in col_type:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Patient",
                    "fhir_field": "birthDate"
                }
            elif fhir_field == "gender" and "string" in col_type:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Patient",
                    "fhir_field": "gender"
                }

# Sauvegarder le mappage
try:
    with open("/home/vagrant/datalake-mavis/provision/spark-jobs/discovery/fhir_mapping.json", "w") as f:
        json.dump(fhir_mapping, f, indent=2)
    print("fhir_mapping.json généré")
except Exception as e:
    print(f"Erreur lors de l'enregistrement: {str(e)}")
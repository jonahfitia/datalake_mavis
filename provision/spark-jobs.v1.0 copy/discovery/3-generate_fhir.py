import json

with open("/home/vagrant/datalake-mavis/provision/spark-jobs/discovery/discovery_report.json") as f:
    report = json.load(f)

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
            if col_name in ["patient_id", "id", "partner_id"]:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Patient",
                    "fhir_field": "id"
                }
            elif col_name in ["name"]:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Patient",
                    "fhir_field": "name.given"
                }
            elif col_name in ["weight", "height", "bmi", "birth_weight", "birth_height", "birth_head_circum", "screatinine", "hb", "rbs"]:
                codes = {
                    "weight": "29463-7",
                    "height": "8302-2",
                    "bmi": "39156-5",
                    "birth_weight": "8339-4",
                    "birth_height": "8302-2",
                    "birth_head_circum": "8287-5",
                    "screatinine": "2160-0",
                    "hb": "718-7",
                    "rbs": "1558-6"
                }
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Observation",
                    "fhir_field": "valueQuantity",
                    "code": codes.get(col_name)
                }
            elif col_name in ["date", "evaluation_date", "appointment_date", "admission_date", "create_date", "write_date", "mammography_last", "pap_test_last", "colposcopy_last"]:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Observation" if "evaluation" in table_name.lower() else "Encounter",
                    "fhir_field": "effectiveDateTime" if "evaluation" in table_name.lower() else "period.start"
                }
            elif "diagnosis" in col_name or "disease" in col_name:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Condition",
                    "fhir_field": "code"
                }
            elif "ward" in col_name or "department" in col_name:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Location",
                    "fhir_field": "name"
                }
            elif col_name in ["gravida", "gravida_number", "abortions", "stillbirths", "full_term", "premature", "cesarean_birth", "vaginal_birth", "menarche", "menopause"]:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Observation",
                    "fhir_field": "valueQuantity",
                    "code": "11612-9"
                }
            elif col_name in ["hiv", "hbsag"]:
                codes = {"hiv": "55277-8", "hbsag": "5196-1"}
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Observation",
                    "fhir_field": "valueCodeableConcept",
                    "code": codes.get(col_name)
                }

# Sauvegarder le mappage
with open("/home/vagrant/datalake-mavis/provision/config/fhir_mapping.json", "w") as f:
    json.dump(fhir_mapping, f, indent=2)

print("fhir_mapping.json généré")
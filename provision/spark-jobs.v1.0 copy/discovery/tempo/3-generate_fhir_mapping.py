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
            if col_name in ["patient_id", "id"]:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Patient",
                    "fhir_field": "id"
                }
            elif col_name in ["name"]:
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Patient",
                    "fhir_field": "name.given"
                }
            elif col_name in ["weight", "height", "bmi"]:
                codes = {"weight": "29463-7", "height": "8302-2", "bmi": "39156-5"}
                fhir_mapping[source_name][table_name][col_name] = {
                    "fhir_resource": "Observation",
                    "fhir_field": "valueQuantity",
                    "code": codes.get(col_name)
                }
            elif col_name in ["date", "evaluation_date", "appointment_date", "admission_date"]:
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

with open("/home/vagrant/datalake-mavis/provision/config/fhir_mapping.json", "w") as f:
    json.dump(fhir_mapping, f, indent=2)

print("fhir_mapping.json généré")
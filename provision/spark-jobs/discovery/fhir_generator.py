import json
from datetime import datetime

def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def apply_mapping(record, mapping):
    resource = {"resourceType": mapping["fhir_resource"]}
    for m in mapping.get("mappings", []):
        source_value = record.get(m["source_column"])
        if source_value is None:
            continue
        fhir_field = m["fhir_field"]
        if "transform" in m:
            if m["transform"].startswith("http://"):
                parts = m["transform"].split("|")
                if len(parts) == 3:
                    url, value, unit = parts
                    value = source_value
                    resource.setdefault("extension", []).append({
                        "url": url,
                        "value" + ("Money" if unit == "KES" else "Quantity"): {
                            "value": float(value) if value else 0.0,
                            "currency" if unit == "KES" else "unit": unit
                        }
                    })
                    continue
                elif parts[0].startswith("http://hl7.org/fhir/sid/icd-10"):
                    source_value = m["transform"].format(**record).split("|")[-1]
                else:
                    source_value = m["transform"].format(**record)
            else:
                source_value = m["transform"].format(**record)
        
        keys = fhir_field.split(".")
        target = resource
        for i, key in enumerate(keys[:-1]):
            next_key = keys[i + 1] if i + 1 < len(keys) else None
            # Gérer les champs qui sont des listes
            if key in ["diagnosis", "coding", "extension", "participant", "identifier", "reasonCode", "note"]:
                if key not in target:
                    target[key] = [{}]
                if next_key and next_key not in ["value", "code"]:  # Continuer dans le dictionnaire
                    target = target[key][0]
                else:  # Rester au niveau de la liste pour l'assignation finale
                    target = target[key]
            else:
                # Gérer les dictionnaires
                target[key] = target.get(key, {})
                target = target[key]
        
        # Assigner la valeur finale
        last_key = keys[-1]
        if isinstance(target, list):
            if last_key in ["code", "value", "display", "text"]:
                target[0][last_key] = source_value
            else:
                target.append({last_key: source_value})
        else:
            target[last_key] = source_value
    
    return resource

def generate_fhir_bundle(discovery_data, fhir_mapping, table_to_include=None):
    bundle = {"resourceType": "Bundle", "type": "collection", "entry": []}
    if not table_to_include:
        table_to_include = [table["table_name"] for table in discovery_data["MAVIS"]]
        print(f"Aucune table spécifiée dans table_to_include. Traitement de toutes les tables : {table_to_include}")
    
    for table in discovery_data["MAVIS"]:
        table_name = table["table_name"]
        if table_name not in table_to_include:
            continue
        mapping = fhir_mapping["MAVIS"].get(table_name, {})
        if not mapping:
            continue
        for record in table.get("sample_data", []):
            resource = apply_mapping(record, mapping)
            resource["meta"] = {
                "versionId": "1",
                "lastUpdated": record.get("write_date", datetime.now().isoformat()) + "+03:00"
            }
            if mapping["fhir_resource"] == "DiagnosticReport":
                resource["status"] = "final"
                if "code" in resource and "coding" in resource["code"]:
                    resource["code"]["coding"][0]["system"] = "http://example.com/test-type"
            elif mapping["fhir_resource"] == "Encounter":
                resource["status"] = "finished"
                resource["class"] = {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                    "code": "AMB" if table_name == "hms_appointment" else "IMP",
                    "display": "Ambulatory" if table_name == "hms_appointment" else "Inpatient"
                }
            elif mapping["fhir_resource"] == "MedicationRequest":
                resource["status"] = "active"
                resource["intent"] = "order"
            bundle["entry"].append({"resource": resource})
    
    return bundle

def main():
    discovery_data = load_json("/home/vagrant/datalake-mavis/provision/spark-jobs/discovery/discovery_report.json")
    fhir_mapping = load_json("/home/vagrant/datalake-mavis/provision/config/fhir_mapping.json")
    table_to_include = ["hms_patient", "acs_imaging_request", "acs_laboratory_request", "hms_appointment", "acs_hospitalization", "prescription_order"]
    
    bundle = generate_fhir_bundle(discovery_data, fhir_mapping, table_to_include)
    
    with open("/home/vagrant/datalake-mavis/provision/spark-jobs/discovery/fhir.json", "w") as f:
        json.dump(bundle, f, indent=2)
    print("fhir.json généré avec succès.")

if __name__ == "__main__":
    main()
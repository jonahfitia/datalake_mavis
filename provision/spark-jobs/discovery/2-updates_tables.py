import json
import shutil

try:
    with open("/home/vagrant/datalake-mavis/provision/spark-jobs/discovery/discovery_report.json") as f:
        report = json.load(f)
except Exception as e:
    print(f"Erreur lors du chargement du rapport: {str(e)}")
    exit(1)

relevant_keywords = [
    "patient", "evaluation", "hospitalization", "ward", "disease", "diagnosis",
    "treatment", "appointment", "clinical", "medical", "laboratory", "imaging",
    "vaccination", "surgery", "medication", "health_service", "perinatal"
]

relevant_tables = {}
for source_name, tables in report.items():
    relevant_tables[source_name] = [
        table["table_name"] for table in tables
        if "error" not in table and any(keyword in table["table_name"].lower() for keyword in relevant_keywords)
    ]

try:
    with open("/home/vagrant/datalake-mavis/provision/config/data_sources.json") as f:
        sources = json.load(f)
except Exception as e:
    print(f"Erreur lors du chargement de data_sources.json: {str(e)}")
    exit(1)

# Backup avant modification
backup_path = "/home/vagrant/datalake-mavis/provision/config/data_sources_backup.json"
shutil.copyfile("/home/vagrant/datalake-mavis/provision/config/data_sources.json", backup_path)
print(f"Backup créé: {backup_path}")

for source in sources:
    if source["name"] in relevant_tables and not source.get("tables_to_include"):
        source["tables_to_include"] = relevant_tables[source["name"]]

try:
    with open("/home/vagrant/datalake-mavis/provision/config/data_sources.json", "w") as f:
        json.dump(sources, f, indent=2)
    print("data_sources.json mis à jour avec tables_to_include")
    for source_name, tables in relevant_tables.items():
        print(f"Tables pour {source_name}: {len(tables)}, {tables}")
except Exception as e:
    print(f"Erreur lors de l'enregistrement: {str(e)}")
    
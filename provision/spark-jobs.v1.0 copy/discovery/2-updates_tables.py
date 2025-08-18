import json

with open("/home/vagrant/datalake-mavis/provision/spark-jobs/discovery/discovery_report.json") as f:
    report = json.load(f)

relevant_keywords = [
    "patient", "evaluation", "hospitalization", "ward", "disease", "diagnosis",
    "treatment", "appointment", "clinical", "medical", "laboratory", "imaging",
    "vaccination", "surgery", "medication", "health_service", "perinatal"
]

relevant_tables = {
    "MAVIS": [
        table["table_name"] for table in report["MAVIS"]
        if "error" not in table and any(keyword in table["table_name"].lower() for keyword in relevant_keywords)
    ],
    "MMT_DB": [
        table["table_name"] for table in report["MMT_DB"]
        if "error" not in table and any(keyword in table["table_name"].lower() for keyword in relevant_keywords)
    ]
}

with open("/home/vagrant/datalake-mavis/provision/config/data_sources.json") as f:
    sources = json.load(f)

for source in sources:
    if source["name"] in relevant_tables:
        source["tables_to_include"] = relevant_tables[source["name"]]

with open("/home/vagrant/datalake-mavis/provision/config/data_sources.json", "w") as f:
    json.dump(sources, f, indent=2)

print("data_sources.json mis à jour avec tables_to_include")
print("Tables pour MAVIS:", len(relevant_tables["MAVIS"]), relevant_tables["MAVIS"])
print("Tables pour MMT_DB:", len(relevant_tables["MMT_DB"]), relevant_tables["MMT_DB"])
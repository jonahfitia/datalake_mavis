import json
import os

# Chemins vers les fichiers
DISCOVERY_FILE = "spark-jobs/discovery/discovery_report.json"
PIVOT_SCHEMA_FILE = "config/pivot-schema.json"
MAPPING_FILE = "config/mapping_sources_to_pivot.json"

def normalize_column_name(name):
    return name.lower().strip()

def main():
    with open(DISCOVERY_FILE) as f:
        discovery = json.load(f)

    pivot_schema = {}
    mappings = {}

    for source_name, tables in discovery.items():
        if isinstance(tables, dict) and "error" in tables:
            continue  # Skip errored sources

        mappings[source_name] = {}

        for table in tables:
            table_name = table["table_name"]
            columns = table["columns"]
            mappings[source_name][table_name] = {}

            for col in columns:
                source_col = col["name"]
                normalized = normalize_column_name(source_col)

                # Ajouter au schéma pivot si nouveau
                if normalized not in pivot_schema:
                    # devine le type simplifié
                    dtype = col["type"].lower()
                    if "int" in dtype:
                        pivot_schema[normalized] = "integer"
                    elif "date" in dtype:
                        pivot_schema[normalized] = "date"
                    else:
                        pivot_schema[normalized] = "string"

                # Ajouter au mapping
                mappings[source_name][table_name][source_col] = normalized

    # Créer dossiers si besoin
    os.makedirs("config", exist_ok=True)

    # Écriture des fichiers
    with open(PIVOT_SCHEMA_FILE, "w") as f:
        json.dump(pivot_schema, f, indent=2)

    with open(MAPPING_FILE, "w") as f:
        json.dump(mappings, f, indent=2)

    print("✅ pivot-schema.json généré dans config/")
    print("✅ mapping_sources_to_pivot.json généré dans config/")

if __name__ == "__main__":
    main()

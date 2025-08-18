# Pipeline global
- Flux : discovery.py → update_table.py (met à jour config) → generate_table.py (génère mapping) → create_pivot_table.py (construit pivot).
- Forces : Automatisé, scalable avec Spark/HDFS, FHIR pour standardisation, TALN pour intelligence.

# Analyse de discovery.py
- Établit un tunnel SSH pour accéder à des bases PostgreSQL distantes.
- Récupère les métadonnées des tables (noms, types, colonnes, échantillons de données, nombre de lignes).
- Utilise un modèle TALN (all-MiniLM-L6-v2) pour filtrer les tables pertinentes basées sur une similarité cosinus (>= 0.6) avec des mots-clés FHIR/RMA (par exemple, "patient", "diagnosis", "hospitalization").
- Sauvegarde les données brutes en Parquet sur HDFS (hdfs://localhost:9000/datalake/raw/...).
- Gère le cache pour éviter les redécouvertes.
- Parallélise les traitements PostgreSQL avec ThreadPoolExecutor.
- Supporte les sources Excel via un reader Spark spécifique.

# Analyse de update_table.py
- Lit le rapport de découverte (discovery_report.json).
- Filtre les tables pertinentes basées sur des mots-clés (mêmes que FHIR/RMA dans discovery.py).
- Met à jour data_sources.json en ajoutant "tables_to_include" pour chaque source.

# Analyse de create_pivot_table.py
- Charge les Parquet raw.
- Mappe les colonnes via fhir_mapping.json.
- Joint les tables sur patient_id (outer join).
- Nettoie les duplications avec coalesce.
- Sauvegarde la table pivot en Parquet.


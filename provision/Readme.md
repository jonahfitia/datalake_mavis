data-lake-project/
│
├── README.md                     # Documentation du projet
├── config/
│   ├── hive-schema.sql           # Création des tables Hive (DDL)
│   ├── hbase-schema.json         # Schéma HBase (colonnes, familles)
│   └── mongo-schema.json         # Schéma MongoDB (structure des documents)
│
├── spark-jobs/
│   ├── elt_raw_ingestion.py      # Ingestion brut → RAW Zone
│   ├── clean_normalize.py        # Nettoyage + normalisation
│   ├── transform_to_pivot.py     # Mapping → Schéma pivot
│   ├── load_to_hive.py           # Chargement dans Hive
│   ├── load_to_hbase.py          # Chargement dans HBase
│   ├── load_to_mongo.py          # Chargement dans MongoDB
│   └── utils/
│       ├── logger.py             # Logging Spark
│       └── uuid_generator.py     # Génération UUID patients
│
├── scripts/
│   ├── create_hive_tables.sh     # Script bash pour exécuter les DDL Hive
│   ├── start_hdfs.sh             # Lancement HDFS
│   ├── start_spark.sh            # Lancement Spark job
│   └── start_hbase.sh            # Lancement HBase shell
│
├── api/
│   ├── hive_api.py               # API pour accéder aux données Hive
│   ├── mongo_api.py              # API pour MongoDB
│   └── hbase_api.py              # API pour HBase
│
└── logs/
    └── spark_logs/               # Logs d’exécution des jobs Spark

start-dfs.sh
start-yarn.sh
start-master.sh

ssh -L 5432:localhost:5432 -p 8090 mmtadmin@102.16.7.154

## INFO
- n'oublie pas export SPARK_DATABASE_{NOM_DB}_PASSWORD
- paraphrase sur ssh_private_key = mavis

## probleme ET SOLUTIONS : 
### pb logique
- 1000 base * 1000 tables
- 102.06 inaccessible depuis mon vagrant
- probleme de connexion de hôte de la base et vagrant
- faudra faire un script d'automatisation des tunnels ssh
- configurer un proxy SOCKS avec SSH
- datetime not serializable en JSON

### Diagnostic en texte libre
- utilisation de NLP pour faire connaitre les 

### difficulté de la decouverte de toutes les contenus de la base 
- script pour la creation des tables to include
* on se focalise sur la RMA 

### hdfs en safemode
- just desctivate this mode

## ETAPE 
- faire marche discovery.py pour le 1 base MAVIS (avec table _to_ include)
- inclue un diagramme de l’architecture logicielle (ex. : ingestion → transformation FHIR → datalake → visualisation)
- 

## OUTILS
-  sentenceTransformers poue le TALN
-  Databricks (script de ça pour le mapping de CIM-10)
-  Azure Text Analytics for Health (pour la classifications des diagnostics)
-  

## MAIN TASK
-   tableau comparatif des outils de datalake (ex. : Databricks vs Azure Data Lake)

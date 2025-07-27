start-dfs.sh
start-yarn.sh
start-master.sh

ssh -L 5432:localhost:5432 -p 8090 mmtadmin@102.16.7.154

## INFO
- n'oublie pas export SPARK_DATABASE_{NOM_DB}_PASSWORD
- paraphrase sur ssh_private_key = mavis

## probleme : 
- 1000 base * 1000 tables
- 102.06 inaccessible depuis mon vagrant
- probleme de connexion de hôte de la base et vagrant
- faudra faire un script d'automatisation des tunnels ssh
- configurer un proxy SOCKS avec SSH
- datetime not serializable en JSON

## ETAPE 
- faire marche discovery.py pour le 1 base MAVIS (avec table _to_ include)
- 
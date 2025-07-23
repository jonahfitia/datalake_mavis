start-dfs.sh
start-yarn.sh
start-master.sh

ssh -L 5432:localhost:5432 -p 8090 mmtadmin@102.16.7.154
paraphrase sur ssh_private_key = mmt_admin

probleme : 
- 1000 base * 1000 tables
- 102.06 inaccessible depuis mon vagrant
- probleme de connexion de hôte de la base et vagrant
- faudra faire un script d'automatisation des tunnels ssh
- configurer un proxy SOCKS avec SSH

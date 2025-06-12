# 🎯 Projet : DataLake Mavis

## 📌 Titre complet
**Réalisation d’un Data Lake interopérable pour analyse et Visualisation des données médicales via application web**

---

## 🧭 Contexte du projet

Dans un contexte où les établissements médicaux génèrent une grande quantité de données hétérogènes (formats, systèmes, technologies), il devient essentiel de centraliser, nettoyer, structurer et rendre accessible ces données pour faciliter leur analyse.

Ce projet propose la conception d'un **Data Lake interopérable** permettant de collecter et analyser les données médicales provenant de plusieurs sources via une application web, avec une attention particulière à l’interopérabilité entre systèmes.

---

## 🎯 Objectif principal

Créer une plateforme complète permettant :
- la **centralisation de données médicales** dans un Data Lake
- la **visualisation dynamique** de ces données via une application web Next.js
- l’**interopérabilité** entre plusieurs systèmes (Hive, MongoDB, HBase)
- la **scalabilité et l’automatisation** des traitements de données (via Spark)

---

## 🧱 Technologies utilisées

| Catégorie        | Technologies / Outils                       |
|------------------|---------------------------------------------|
| 🖥️ Frontend       | Next.js, D3.js, i18n, Axios, Tailwind        |
| 🔄 Backend        | Node.js, Express.js (pour les APIs Hive/Mongo/HBase) |
| 🧪 Traitement      | Apache Spark (PySpark), Logging              |
| 🧊 Stockage Big Data | Hadoop HDFS, Hive, MongoDB, HBase           |
| 🔐 Authentification| NextAuth.js + JWT                           |
| 🖥️ VM / Provision | Vagrant, VirtualBox                         |
| 🌐 Réseau         | VPN, protocole sécurisé (HTTPS, SSH, etc.)   |

---

## 📆 Suivi et planification

📄 **Lien vers le timesheet de planification**:  
👉 [Google Sheets - Timesheet du projet](https://docs.google.com/spreadsheets/d/1JWz_dfxnkArTi9TRiZZPBILCvF8YHNExeXLZ4VoAGos/edit?gid=484971601#gid=484971601)

📄 **Lien vers .box de VM**:  
👉 []()

---

## 📋 Liste des tâches principales

### 1. Initialisation
- Prise en main du projet
- Création VM Vagrant + provisioning
- Installation des environnements (Hadoop, Hive, MongoDB, Spark…)

### 2. Étude & Architecture
- Étude sur l'architecture d’un Data Lake
- Étude réseau (WAN, VPN…)
- Schéma d’interconnexion des hôpitaux
- Mise en place des accès API (Hive, Mongo, HBase)

### 3. ETL / Ingestion / Transformation
- Création de BDD fictives
- Nettoyage & Normalisation des données
- Transformation selon un schéma pivot
- Intégration Hive, MongoDB, HBase
- Génération d’UUID patient

### 4. Frontend - Visualisation
- Initialisation Next.js
- Authentification
- CRUD utilisateurs
- Visualisation avec D3.js (graphes dynamiques)
- Recherche / Pagination / Filtres / UI responsive

### 5. Tests & Déploiement
- Test de cohérence des données
- Test des APIs
- Export de la VM `.box`
- Test d’installation sur d'autres postes
- Déploiement de l’application web
- Documentation et formation utilisateur

---



# Data Warehouse Project

Conception et optimisation d'un Data Warehouse moderne avec PostgreSQL | ETL, Modélisation de Données et Analytics pour la Business Intelligence et le Big Data.

## Table des matières

- [Description](#description)
- [Fonctionnalités](#fonctionnalités)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Utilisation](#utilisation)
- [Structure du Projet](#structure-du-projet)
- [Tests](#tests)
- [CI/CD avec Astro](#cicd-avec-astro)
- [Contributions](#contributions)
- [Licence](#licence)

## Description

Ce projet vise à concevoir un Data Warehouse moderne en utilisant PostgreSQL. Il inclut des pipelines ETL, des modèles de données optimisés et des outils analytiques pour répondre aux besoins de la Business Intelligence et du Big Data.

## Fonctionnalités

- **ETL** : Extraction, Transformation et Chargement des données.
- **Modélisation** : Création de schémas optimisés pour les requêtes analytiques.
- **Analytics** : Génération de rapports et visualisations.
- **Automatisation** : Utilisation d'Airflow pour orchestrer les pipelines de données.

## Prérequis

- Python 3.10 ou supérieur
- PostgreSQL
- Docker et Docker Compose
- Astro CLI
- Les dépendances listées dans `requirements.txt`

## Installation

### 1. Clonez le dépôt :
```bash
git clone git@github.com:jeanmarie237/data-warehouse-project.git
cd data-warehouse-project
```

### 2. Configurez l'environnement virtuel :
```bash
python -m venv my_env
source my_env/bin/activate
```  

### 3. Installez les dépendances :
```bash
pip install -r requirements.txt
```

### 4. Initialisez Astro :
```bash
astro dev init
```

## Utilisation : 
```bash
astro dev start
```
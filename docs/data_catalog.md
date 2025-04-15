# Data catalog for gold layer schema 

## Présentation 
Présentation
La couche Gold représente les données transformées et structurées pour répondre aux besoins métier. Organisée selon un modèle en étoile (star schema), elle se compose de :

- **Tables de dimensions** (descriptions des entités métier)
- **Tables de faits** (mesures et indicateurs clés)

Cette modélisation permet une analyse optimisée et une génération efficace de rapports.

```sql
-- gold.dim_customers
-- Table de dimension des clients (données enrichies)
```

### **1. gold.dim_customers**  
**Description**  
Stocke les profils clients avec données démographiques et géographiques.  

**Schéma**  

| Colonne           | Type   | Description                                  |
|-------------------|--------|----------------------------------------------|
| customer_key      | INT    | [PK] Clé technique unique (surrogate key)    |
| customer_id       | INT    | Identifiant métier unique                   |
| customer_number   | TXT    | Référence client (ex: "CLI-12345")          |
| first_name        | TXT    | Prénom                                      |
| last_name         | TXT    | Nom                                         |
| country           | TXT    | Pays (ex: "France")                         |
| marital_status    | TXT    | "Marié(e)", "Célibataire", etc.             |
| gender            | TXT    | "Homme", "Femme", "Autre"                   |
| birthdate         | DATE   | Date de naissance                           |
| create_date       | DATE   | Date création fiche                         |

**Notes techniques**  
- 🔑 **Clés** :  
  - `customer_key` pour les jointures techniques  
  - `customer_id` pour l'identification métier  
- 📊 **Usage** : Optimisé pour l'analytique (BI, reporting)  

---

```sql
-- gold.dim_products
-- Table de dimension des produits (attributs et classifications)
```

### **2. gold.dim_products**  
**Description**  
Contient les informations produits avec leurs attributs et hiérarchies de catégorisation.  

**Schéma**  

| Colonne              | Type   | Description                                  |
|----------------------|--------|----------------------------------------------|
| product_key          | INT    | [PK] Clé technique unique                   |
| product_id           | INT    | Identifiant métier du produit               |
| product_number       | TXT    | Code produit (ex: "PROD-1002")              |
| product_name         | TXT    | Nom complet du produit                      |
| category_id          | TXT    | Référence de catégorie                      |
| category             | TXT    | Catégorie (ex: "Vélos")                     |
| subcategory          | TXT    | Sous-catégorie (ex: "VTT")                  |
| maintenance_required | TXT    | "Oui"/"Non" pour maintenance                |
| cost                 | INT    | Coût de base (en unités monétaires)         |
| product_line         | TXT    | Gamme produit (ex: "Route", "Ville")        |
| start_date           | DATE   | Date de mise en vente                       |

---

```sql
-- gold.fact_sales
-- Table de faits des ventes (données transactionnelles)
```

### **3. gold.fact_sales**  
**Description**  
Enregistre les transactions commerciales pour l'analyse des performances.  

**Schéma**  

| Colonne         | Type   | Description                                  |
|-----------------|--------|----------------------------------------------|
| order_number    | TXT    | Numéro de commande (ex: "CMD-7841")         |
| product_key     | INT    | [FK] Lien vers dim_products                 |
| customer_key    | INT    | [FK] Lien vers dim_customers                |
| order_date      | DATE   | Date de passation                           |
| shipping_date   | DATE   | Date d'expédition                           |
| due_date        | DATE   | Date d'échéance de paiement                 |
| sales_amount    | INT    | Montant total (unité monétaire)             |
| quantity        | INT    | Quantité vendue                             |
| price           | INT    | Prix unitaire                               |

**Annotations**  
- [PK] = Primary Key, [FK] = Foreign Key  
- Dates au format `AAAA-MM-JJ`  


# 🌟 Databricks_SparkSQL
Welcome to my Spark SQL project, where I explored Azure Blob Storage, Azure Databricks, and Spark SQL on an e-commerce dataset. \
This repository contains notebooks and learnings from working with Spark SQL in a structured and professional data engineering environment.

# 📌 Project Overview
**Dataset:** E-commerce dataset (uploaded to Azure Blob Storage > containers).\
**Platform:** Azure Databricks.\
**Tech Stack:** Spark SQL, Azure Blob Storage, Delta Lake, Databricks Security Features.\
**Goal:** To build Spark SQL tables, perform transformations, apply data security, and explore Delta Lake features for versioned data.


---


##  1. 🗃️ Data ingestion and Set up
- Dataset Upload and Azure Blob Integration. 
- Uploaded it to Azure Blob Storage in a container.
- Created and configured external data credentials in Databricks.
- Connected the Databricks workspace to Azure Storage using access keys and secrets.

The snapshot below illustrates how I created an external table in the Catalog. The URL specifies the container location where the data is stored & storage credentials were created using an Access Connector for Azure Databricks, with the Resource ID used to grant the necessary permissions.

![Screenshot 2025-06-26 182107](https://github.com/user-attachments/assets/d7380da4-d0ba-40a3-8cbf-75453aadea8d)


## 2. 🏗️ Catalog, Schema & Table Setup
- Created Catalogs and Schemas in Databricks.
- Built external and managed tables using Spark SQL.

![Screenshot 2025-06-26 184137](https://github.com/user-attachments/assets/02991b8e-9933-4240-b31c-ca7583808e76)

## 3. 📊 Temporary Views and DataFrames
- load data as DataFrames.
- Created temporary views and global temporary views.
- Understood the difference between external tables, managed tables.

The key difference between creating a managed table and an external table is specifying the storage location during table creation.\
#### External Table:-
![Screenshot 2025-06-26 184529](https://github.com/user-attachments/assets/1079cfc2-1d71-4964-83eb-479d83f9d696)

#### Managed Table:-
![image](https://github.com/user-attachments/assets/eb72f848-01be-4e49-bf6d-f14b8c847238)

A quick note on the difference between managed and external tables in Azure Databricks:
With managed tables, Databricks handles both the data and metadata it stores everything in its own DBFS-managed storage.
For external tables, you provide the storage location, and only the metadata is managed by Databricks. The actual data stays in your external storage, like ADLS.
So, if you don’t define a path, it’s managed. If you do, it’s external.

## 4. 🔄 Upsert & Merge
- Implemented Upsert logic using MERGE in Spark SQL.
- Learned about merge conditions, matched vs unmatched, and real-world usage in slowly changing dimensions (SCD).

![image](https://github.com/user-attachments/assets/911204b2-8951-4016-825e-9a7e47bbeace)

## 5. 🧠 Functions (UDF) & (UDTF)
#### Created:
- Scalar UDFs: to apply custom logic row-by-row.
- Table-valued UDFs (UDTFs): for returning tables from custom logic.\
Explored when and where to use each type effectively.

#### UDF:
![Screenshot 2025-06-26 190322](https://github.com/user-attachments/assets/bb0b2f64-fa72-4c39-9b39-ac9ac651c252)

#### UDTF:
![Screenshot 2025-06-26 190642](https://github.com/user-attachments/assets/30609410-6f7a-49cb-af6c-97809f2b6471)

## 6. 🛡️ Dynamic Data Masking (DDM)
- Learned to secure sensitive data using dynamic masking:
- Set up Access Groups under Identity & Access.
- Created functions that reference group roles.
- Applied role-based masking to fields such as email, credit card, etc.


## 7. 🧩 Row-Level Security (RLS)
- Implemented row-level filtering in Spark SQL using: Security groups & Mapping table

## 8. 🔁 Delta Lake & Time Travel
Introduced Delta Lake to the project.
Learned about:

- Data versioning
- Time travel
- Rollback and audit-friendly queries

Performed operations using VERSION AS OF and TIMESTAMP AS OF.

#### Data versioning:-
![image](https://github.com/user-attachments/assets/e2545f0f-d2b5-483b-8721-166f127bf8aa)


#### Time travel:-
![image](https://github.com/user-attachments/assets/453cc307-e4f3-46bd-85e4-9e5f60762ac9)

















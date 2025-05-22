# 🛠️ DataHub Migration Project

Build a unified **DataHub** that ingests, standardises, and catalogues data from  
**Qlik Replicate, SQL Server, Salesforce, OneStream, CSV files, and SharePoint** using AWS Glue Spark jobs.

---

## 🔍 Overview
This project creates a metadata-aware DataHub layer that:

* **Ingests** data from six enterprise systems.  
* **Transforms** raw data to a consistent, analytics-friendly schema.  
* **Persists** results as Parquet or Iceberg tables on Amazon S3.  
* **Catalogues** lineage and metadata for data discovery and governance.

---

## 🏗️ Architecture

```text
        +-------------+       ┌──────────────┐
        | Qlik CDC    |──────▶│ Glue Job:    │
        | Files on S3 |       │ etl_qlik     │
        +-------------+       └──────────────┘
              ⋮
+----------------+      ┌──────────────┐
| SQL Server     |────▶ │ etl_sqlserver│
+----------------+      └──────────────┘
              ⋮
+----------------+      ┌──────────────┐
| Salesforce API |────▶ │ etl_salesforce
+----------------+      └──────────────┘
              ⋮                          ┌─────────────┐
      (other jobs)  ─────────────────────▶│ S3 / Iceberg│
                                          └─────────────┘
                                                │
                                                ▼
                                      +--------------------+
                                      | DataHub / Athena   |
                                      +--------------------+

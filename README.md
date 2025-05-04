# Batch Processing
## `NYC-Taxi-Data`
===============================

This repository implements an end-to-end data processing system for New York City Yellow Taxi records, leveraging modern cloud technologies:

Data Ingestion: Raw taxi data (Parquet format) from Azure Data Lake Gen2

`Core Processing`:

Delta Lake for reliable storage (ACID transactions, schema enforcement)

Delta Live Tables (DLT) for orchestration and pipeline management

`Transformations`:

Bronze → Silver → Gold layer architecture

Data quality checks, time-based partitioning, and business-ready aggregates

`Tech Stack`:

Cloud: Microsoft Azure (Data Lake Gen2, Blob Storage)

Processing: Databricks (Python, SQL)








## `system Components`

#### 1. Mount Storage
- **File**: `Mount_Storage.py`
- **Purpose**: Establishes connection between Databricks and Azure Data Lake Gen2
- **Key Actions**:
  - Mounts `taxidata` container from Azure Storage `taxidatalake01kenne` to DBFS
  - Access path: `/mnt/datalake`
- **Requirements**:
  - Azure service principal credentials

#### 2.ETL Pipeline (DLT)
- **File**: `Building_ETL_Pipeline_DLT_YellowTaxis.sql`
   This SQL file implements an ETL (Extract-Transform-Load) pipeline structured into three layers (Bronze, Silver, Gold) using Delta Live Tables (DLT) in Databricks. It orchestrates the transformation of raw data into analytical insights ready for Business Intelligence.
  #### Pipeline Key Components :
  1.Bronze Layer (YellowTaxis_BronzeLive)
    - **Source** : Raw Parquet data stored in Azure Data Lake (/mnt/-datalake/raw/YellowTaxisParquet).
    - **Processing** :
       - **Cast PickupTime and DropTime to TIMESTAMP**
       - **Add metadata columns**:
           - **FileName** : Source file name via INPUT_FILE_NAME()
           - **CreatedOn** : Ingestion timestamp via CURRENT_TIMESTAMP()
       - **Partitioning** Optimize queries by `VendorId`.
  2. Silver Layer (YellowTaxis_SilverLive)
    -  **Processing** :
       - **Data Cleansing**:
           - ** Generated columns** `PickupYear` , `PickupMonth`, `PickupDay`(from PickupTime).
           - **Data Quality Constraints**:
             - `Valid_TotalAmount` : Drop rows where TotalAmount is null or ≤ 0.
             - `Valid_TripDistance` :  Drop rows with TripDistance ≤ 0.
             - `Valid_RideId ` : Fail pipeline if RideId is invalid (null or ≤ 0).






## `Pipeline Architecture`
[Azure Data Lake Parquet]  

       │ 
    data ingestion
       ▼
[Azure Databricks]

       │ 
    data processing
       ▼   

[DLT Databricks]

       │ 
    data storage 
       ▼  
[Delta table store in Azure data lake gen2] 




## `Setting up the System` 
  #### Prerequisites
  1- Setup Azure Data Lake Gen2 account with credential
  - `create Azure container in Data Lake`
  - `Upload files into Data Lake`

2-  Create Azure AD App(Service Principal)
- `save  Client Id,Client Secret & Tenant Id` to create the connection and monting

3- Grant access with Role(Storage Bolb Data Contributor) to Azure AD App to read and write File From Data Lake  

4- Azure Databricks Workspace Setup

  a- Create & Launch Workspace
  - `Provision an Azure Databricks workspace via Azure Portal `
  - `Launch the workspace to configure pipelines`

  b- Cluster Configuration
  - `Set up an all-purpose interactive cluster for development/testing` : Allows running multiple notebooks/pipelines on a single cluster
     Optimizes cost by avoiding repeated cluster startups


## `Screenshots`

#### Star Schema Yellow Taxi
![Star_Schema_Taxi-Data.drawio.png](Images%2FStar_Schema_Taxi-Data.drawio.png)

#### YellowTaxis_SilverLive Data Quality
![YelloTaxis_SilverLive.png](Images%2FYelloTaxis_SilverLive.png)

#### YellowTaxis_SummaryByLocationGold Data Quality
![YellowTaxis_GoldLive.png](Images%2FYellowTaxis_GoldLive.png)

#### Yellowtaxipipeline liste
![YellowTaxiPipeline.png](Images%2FYellowTaxiPipeline.png)

                       








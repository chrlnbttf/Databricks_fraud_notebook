# Databricks_fraud_notebook

1. Datasources:

- Subject: Bank transactions and frauds
Kagel: https://www.kaggle.com/datasets/kartik2112/fraud-detection
File: fraudTrain.csv renamed fraudDetection.csv
Folder: Credit Card Transactions Fraud Detection

- Subject: Connection logs
Generated data

- Subject: Client profiles
Kagel: https://www.kaggle.com/datasets/arjunbhasin2013/ccdata
File: CC GENERAL
Folder: Credit Card Dataset for Clustering


2. Purposes:

The aim of this notebook is the use of Microsoft Azure Databricks for data engineering study purposes, involving the classical phases of data curation - ingestion (here in 3 different formats), transformation, integration - and first analyses, prerequisite of machine learning projections (possibilities to expand the project upon request)


3. Plan:

- Import of libraries and modules
- Data Ingestion: Regarding Transactions (CSV) and client profiles (PARQUET)
- Data ingestion: Creation of Log frequency dataset and Ingestion of Logs (JSON=>DELTA)
- Dataframe Insight menu
- Data Transformation: Injection of credit card numbers randomly and inheritated from transactions_df into profiles_client to create a common key
- Data Transformation: Temporal information-based matching to create a common key between transactions_df and logs_df
- Data Integration
- Data Analyses: Creation of flags as potential fraud indicator (amount per user, logs frequency, unusual locations etc)

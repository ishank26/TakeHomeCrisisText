# Steps to execute:
1. Login into databricks account (https://community.cloud.databricks.com/)
   1. Username: ishank269@gmail.com
   2. Password: wormholE@17
2. Naviagte to workspace
   
Note: If compute cluster is terminated. Create and attach new cluster from topright button in each notebook.


# Pre-requisite 
1. Create a compute cluster with Spark 3.3.2
2. Python runtime enviroment 3.9.x

# Bronze Layer (Notebook: bronze_layer)
1. Upload data to shared directory from notebook
2. Read data to a spark dataframe.
3. Check schema of data to see what fields it contains.
4. Based on documentation validate if loaded dataframe contains any missing columns
5. Define an expected schema with all fields and respective data types. 
6. Apply a casting on each column of dataframe to convert it to expected data type. If the expected column is missing add it intialised with null values. Drop any extra columns
7. The dataset documentation has listed "-9" as an invalid/unknown or bad data. Replace this value in all columns with a Null.
8. Check for total null counts in all fields. Based upon this frequency, Age has lowest null count among the demographic fields such as- Education, Age, Ethnicity, Race, Gender. 
9. Drop all rows where age is null because it has the lowest null frequency among demographic fields as well as it is an important column for analysis of data. 
10. Create bronze delta table:
    1.  Retention days: 60, Z-order field: CaseID, Partition field: Age (Age is low cardinality with an apporx. uniform distribution. Assuming a lot of queries will be age specific)
    2.  Mode: append, as we want to append new data to existing table
    3.  Table name: RawTable

# Silver Layer (Notebook: silver_layer)
1. Create a copy of bronze delta into a datframe
2. Apply transforamtions for GENDER, RACE, ETHNIC, MARSTAT, EMPLOY
   - Transformations are applied before creating delta table to reduce update operations on table itself. This will ensure better consistency in case of network or application failures. 
3. Create silver delta table:
    1.  Retention days: 60, Z-order field: CaseID, Partition field: Region (Region is used for partioning as it has a approx unifrom distribution with low cardinality)
    2.  Mode: append, as we want to append new data to existing table
    3.  Table name: TransformTable
4. Validate schema with expected schema.
5. I couldn't find columns for min max and z normalisation in dataset but have provided a sample code. A scikit learn pipeline is constructued and excuted for this task.
6. Stratified sampling is performed based on GENDER, RACE, ETHNIC, MARSTAT, EMPLOY ratio. 
   1. Train/ Val and Test is split based on ratio- 07/0.1/0.2
   2. First the train and test is split from dataset.
   3. From train we split validation set.
   4. Create a report of count distribution afer splitting.
   5. Save the dataset splits in an output directory. 
   6. Save the report to specified directory.
   7. For all the above task in sampling and splitting I created a scikit learn pipeline.
   
# Gold Layer (Notebook: gold_layer)
1. Create a copy of silver delta into a dataframe
2. Apply region transformation where region values are converted to string based on business logic.
3. Create gold delta tables:
    1. Retention days: 60, Z-order field: CaseID, Partition field: Gender
    2. Mode: append, as we want to append new data to existing table
    3. Table name: ClientTable<RegionName>
4. Perform schema check for each table.


Note: For choosing z-order high cardinality index such as CASEID is chosen. For paritionin as low cardinality index with approximately uniform distribution is chosen. 


# Optimizations:
1. WWe can use streaming in delta table for live updates to each layer if the data source changes frequently. 
2. Transaction logs for each delta can be backed up for disaster recovery in case of failures. 
3. Set periodicly perform vacuum operation to remove any stale logs before retention horizon.
4. Batch job can be cerated for each stage which can sequentially trigger each notebook.
5. Use logging and more try/catch to log any exceptions. Store these logs for monitoring. 
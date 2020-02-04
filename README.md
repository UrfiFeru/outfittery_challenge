Please read this as I have defined my working in detail and have also mentioned any improvements which are not reflected in the code due to time/tool limitation at my disposal.
## Overview
For Outfittery Challenge, I used ELT philosophy to build the engineering pipeline. 
Spark and Airflow are used for building Extraction and Loading of data into the data warehouse for which I used PostgreSql for simplicity/availability but desire to use any big data platform if this were to run in production.
DBT (Data Build Tool) is used for transformation. This is the tool I found which empowers developers to integrate Data Ops philosophy in their development environment. The benefits of DBT are also mentioned below and also answers the bonus question of Nakul 😊
## Alternative to Airflow (Bonus Question):
Once all the sources have been mapped into the landing layer for the warehouse, all the queries should be mapped onto DBT (Data Build Tool). This tool is built for providing developers including engineers and analysts the facility to use Data Ops paradigm in their development for the data warehouse. It essentially provides the developers to transform data once it is already loaded into data warehouse.
Some of the major functionalities which it provides which I believe makes it better than Airflow for managing SQL transformations are:\n
•	Provide ability to define data lineage by providing dependencies on table.\n
•	Clean code management as the general rule of defining a single transformation via a single select statement in code makes the code atomic, better to maintain and is optimized for big data environment.\n
•	Ability to provide description within code develops a meta repository and makes data discovery for analysts much easier.\n
•	It provides version control facility, making it easier to oversee development over time and makes it easy to fix bugs.\n
•	It also provides a kind of CI/CD functionality by providing separation of dev and prod environments.\n
For airflow alternatives pertaining to extraction and load of data, I could not find any in general. There definitely are some tools which interact with certain data sources better than every other tool, however when writing custom code, I find it best to use Apache Airflow. It really depends on where most of the company’s data sources lie on and which is the data warehousing platform the company is using.
## Extraction and Load
I unzipped the files and used each file as a source to my data warehouse.
extract_load_outfittery.py uses Spark to load data and contains a separate function for each data source. Every function goes like this:
•	Define the table Name to load the data in for that data source.
•	File Path where the data source file is placed.
•	Manual definition of Schema for Data source. 
•	Call extract_load function and pass the above 3 parameters to the function. 
## Extract_Load Function In the Code
This function uses the Spark functionality and spark-xml library to load data into the warehouse db. A connection string for the postgre server is defined here and is used to create db engine to do the same. Then Spark Session is created to get data from the file to a Spark Data Frame. PySpark DataFrames are then converted to Pandas DataFrames and then loaded to PostegreSql Database using Truncate and Load Methodology, explained in detail below.
Note: It is better for management and security purpose to use PostgresSQlHook provided by Airflow to define any connections. Couldn’t use it due to restricted time.
## Assumption Regarding Loading Mechanism
It is assumed that in the source, full data is present for every table at any point in time and is readily available to be loaded into the destination.
Each file in the source is loaded into Landing layer of the warehouse. The code has been developed such that the table is truncated and data is inserted every time. However, this approach does not maintain data history and loses the change in comparison with previous data and would also not work for large amount of data in any relational DB such as PostegreSql as processing all of the data every single time is not feasible. What I would propose is to load this data into a big data warehousing platform such as Google Big Query or Alibaba's Maxcompute and use Date (and Hour) partition for every load. This makes data snapshot for every day readily available in the same table enabling any analyst to use the partitions to find the change between hourly or daily interval. A partition Lifecyle can be set which can delete partition after every x days making sure storage does not go out of size very soon. 
## Table Names In Landing Layer
These are the table names corresponding to every data source with the same name.
•	badges_landing
•	comments_landing
•	posts_landing
•	postlinks_landing
•	users_landing
•	votes_landing
•	posthistory_landing
## Airflow Orchestration
airflow_outfittery.py contains code for airflow used for orchestrating the Extraction and Loading of data into Postgres. It contains tasks for every data source which are all called in parallel. Once every task is finished, dbt code is called to run dbt code for transformation within the warehouse.
The dbt code is set to be dependent on every loading task in the staging, layer hence the dbt task would only be started after every other task has been completed.
## Unit Test Cases for Data Load
I had thought of multiple integrity and quality checks but could not implement it as could not find enough time to do that. I can implement them if asked by Nakul at a later stage😊
•	In the code I used manual schema specification to identify that the values coming in the source is the same as which should be coming. For integer values, only integer values should come. columnNameOfCorruptRecord should be used to identify any corrupted records.
•	test_dagbag_import_time has been written/copied to test if there are any data sources which are taking more time than usual to load through a defined threshold.
•	For the transformation part, dbt can be used to create data quality checks, the most basic ones including duplicate and null value checks.
## Transformation
At first, Base Models in dbt are created so that in case of any change in landing layer of the data warehouse, the pipeline does not break as the developer first need to make changes in the dbt model for the changes in the source to reflect in the corresponding pipelines.
When I used spark-xml library, the column names have "_" appended with them so all the base models have been renamed to remove underscore from the column names. They are created as a view on top of landing tables. 
Dbt_utls package has also been imported in dbt config for use with date functions. 
## Further Analysis Columns
User_Transformed.sql contains the required columns for analysis. Logic for every column has been written in the code and all but 1 (Average number of comments per month) has been tested as the datediff function is not compatible with PostgreSql in dbt, which hindered my logic for this column. Column-names should self-sufficient in the code to understand which part has been referred in that column.
The amazing lineage functionality for dbt has been used via ref function to make sure that the data is only transformed when it is present in the landing/base models.

Note: If you want to see how dbt works, go to this URL and ask me for credentials if prompted.
https://cloud.getdbt.com/#/accounts/2717/projects/2741/develop/

Postgres DB is configured on this URL
https://api.elephantsql.com/console/3e8dbfe6-565e-4277-b64c-21d3315d7c5a/details

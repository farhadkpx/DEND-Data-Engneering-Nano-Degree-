## `Udacity Data Engineering Nanodegree` - `Capstone Project`

### Farhad Md Ahmed

## `Project Summary:`
This is my Capstone project for Udacity `Data Engineering Nanodegree program`. Here I'd showcase my overall learning consequence from this course. We're given five datasets large and medium range in size, our assigned directive is to go through the data exploration phases and extract out relevant valuable insight from these datasets. I personally think, some of these datasets are not directly relevant to my  'immigration pattern' analysis(for instance ..Temperature data). The overall confluence of my analysis is to develope a fact(`immigration-table`) and dimension table connected correlations to build a star-schema data pipleline model.

Our scrutiny on these dataset should help data analysts to understand immigration trends and patterns which in turn will help(Govt or Policy makers) to make better decisions on immigration policy, subsequently to help other industry like labor intensive businesses, farming, tourism, construction and many more.

As a data engineer my job with this project is to develope a final clean data product ready for instant intricate data analysis.

### `project steps:`

`Step` `1:` **Scope the Project and Gather Data**

`Step` `2:` **Explore and Assess the Data**

`Step` `3:` **Define the Data Model**

`Step` `4:` **Run ETL pipeline to Model the Data**

`Step` `5:` **Addressing some hypthetical scenario**

`Step` `6:` **Analytical query display**


### `Step 1:` Scope the Project and Gather Data
--------------------------------------------------------------------------------------------------------------------------------------
I'll create multiple clean data sets with this project those should be ready for immediate analysis to quickly gather insights from the data sets. The clean data tables will be loaded through a data pipeline into S3-bucket or in a Amazon Redshift  as parquet files using PySpark. On this progression, I'd also use an Apache Airflow processes to automate my data pipeline. 

**Describing Data Sets used:**

`I94 Immigration Data:`
The immigration data comes from the [US National Tourism and Trade Office](https://www.trade.gov/national-travel-and-tourism-office). It includes detail information about immigrants entering into United States as such year and month, arrival and departure dates, age and year of birth of immigrant, arrival city, port, current residence state, travel mode (air, sea), visa type etc.

`World Temperature Data:`
This dataset came from Kaggle. This dataset includes both world and US City temperatures. You can read more about it from [kaggle](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data).

`World City Demographic Data:`
The demographic data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). It includes demographic information about world and US cities, with median age, total population, and specific populations (male vs female, foreign-born, different races columns) which is in csv format.

`Airport Code Table:`
This table contains information about US airports with their details as such airport type, location and GPS coordinates. It comes from [datahub](https://datahub.io/core/airport-codes#data), which is a in csv file format.

`LABEL description file:`
This is a SAS data label file decodes column labels corresponds to some 'immigration' data columns. Since lot of data-columns with the immigration dataset came in a coded format, this label file will help us create an intelligible face to those columns. [data source](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/blob/main/DEND_Capstone_Project/Data_Tables/I94_SAS_Labels_Descriptions.SAS)

We'll extract out Country Code, State Code, Port City and Port Codes from this table to match with our immigration data sets.

`A.` `Country Code:` For instance we'll extract out country names corresponding to `i94CIT` &  `i94RES` codes that are found in the immigration data. 

`B.` `State Code:` This maps the `I94ADDR column` in the immigration table.

`C.` `Port City:` We'll find port city name in connection with `i94PORT` where the immigrants came first.

`D.` `Port Code:` The port code will identify the `I94PORT` column in the immigration table. 

`E.` `Visa Codes:` Visa code will decode `i94VISA` column into visa category type issued to an immigrant.

`F.` `Travel Mode:` will show in what transportation mode was used by an immigrant from `I94MODE` column in the immigration table.


### `Step 2:`  Explore and Assess the Data
--------------------------------------------------------------------------------------------------------------------------------------
All 5 tables needed some level of data cleaning. These data quality issues are mostly ensued from missing data values, duplicate data, mismatch data type, null values etc. Here I'll explain in limited detail what kind of data cleaning was done with these tables.

I used `PySpark` in reading, exploring and cleaning data tables in my Jupyter [notebook](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/blob/main/DEND_Capstone_Project/Final_Capstone_Project_Data_Analysis.ipynb)

#### `DATA-QUALITY` & `CLEANING:`

`A.` **`I94 Immigration Data:`  `Data quality issues`:** 
+ Total number of records/rows are 3096313.. and the number of distinct immigration_id's (cicid) was also.. 3096313 which suggests that the column `cicid` is unique without any duplicated ID-columns.
+ The data spans arrival dates in the month of April for the year 2016. The dates provided are SAS dates format...
+ The arrival & departure dates for all records are in string format not date?
+ There are 7 columns (DTADFILE, VISAPOST, OCCUP, ENTDEPA, ENTDEPD, ENTDEPU, DTADDTO) all are coded with values not clear what they meant?

**`Cleaning steps:`**
+ Practically I renamed all the columns for clear understanding of the columns-tables.
+ Transformed (arrival & departure) columns from SAS dates to Python date format.
+ Created with assignment 2 different tables(visa category, mode of travel) for reducing data ambiguity.
+ Developed extensive dated segmentation of both arrival & departure date columns.
+ Eliminated all unnecessary(7) columns from the immigration table.


`B.`**`GlobalLandTemperaturesByCity.csv:` `data quality issues:`**
+ Number of records with this table: 8,599,212 rows and 8 columns (huge data set).
+ The `date` column recorded since 18th century(from year `1743` to `2013`)..(wide range issues).
+ We had 364,130 row temperature values were missing out of more than 8 million rows of data.
+ We have 159 distinct countries and 3448 city (wide scope of locaitons).
+ All column values are given string-data type?

**`Cleaning steps:`**
+ I changed the `dt` column to `date` type.
+ Converted `AverageTemperature`, `AverageTemperatureUncertainty` to float-data-type.
+ Converted Year, Day, Month_Num to `int` and Month_Name to `string` data type.
+ I chose to limit date access from year `2000` to available.
+ Disposed of all the null-temperature rows from the table. Since temperature table without any value doesn't make any sense?
+ Scoped my analytic temperature data limited to USA only.


`C.` **`US-Cities-Demographics.csv` `Data quality issues`:** 
+ Total number of records with this table was 2891 rows and 12 columns.
+ Good news, only 3 columns has null values in range (13 to 16).
+ All the data column-type came with `string-format`?
+ Distinct city count 567 and state count is 49.
+ 5 distinct Races are embedded with this dataset on 'Race-column'.

**`Cleaning steps:`**
+ So I had change columns to appropriate data types (int, float).
+ Pivoted out demographic race data into five different columns. 
+ Chose conversion of upper(City & State) columns for clarity.

`D.` **`Airport Code:` `data quality issues:`**
+ Total number of records is 55075 with 14 columns. 
+ There were only 9189 records have the [iata code](https://airportcodes.io/en/iata-codes/#:~:text=What%20are%20IATA%20codes%3F%20An%20IATA%20code%2C%20consisting,Airport%20has%20the%20%22LHR%22%20as%20the%20IATA%20code.?msclkid=513518aabaac11ec81151a8894006df3) value in it.
+ We had 5 columns with large number of null values in it.
+ For instance `iata_code has 45886` and `local_code column has 26389` number of `null values`.
+ There are `279 airports` was closed.
+ Number of airports appear more than once in the data set.
+ `Latitude` and `longitude` are in a single column separated by commas.

**`Cleaning steps:`**
+ I renamed all the column to make them more menaningful.
+ I've removed the airport identification codes(iata_code) those are missing.
+ Excluded `iata_Codes` are came with `zero` values in it.
+ Eliminated airports those are closed and duplicated.
+ Separated longitude and latitude data for clarity, which increased 1 additional column.

`E.` **`I94_SAS_Labels_Description file:`  `data quality issues:`**
+ This is a single file with SAS data format came with coded label related to City, State, Port and Country codes.
+ All those columns are embroiled with extensive regular expression issues.
+ Data types came with string type.
+ Each columns are mired with extensive regular expression mixed up.

**`Cleaning steps:`**
+ I had to use rigorous regular expression programming steps to clean those rows.
+ I used slicing, splitting, stripping & replacing functions to clean codes.
+ I cast some string data type to integer type.
+ Eventually I came up with 3 different data-label columns(Port, Country, State).

### `Step 3:` Define the Data Model
---------------------------------------------------------------------------------------------------------------------
+ **Map out the conceptual data model and explain why you chose that model.**

I chose to build a star-schema data model for creating and developing my data pipeline. The apparent inadequate correlation among my fact('immigration table') and all the dimension tables made it difficult to create a star schema model. To limit number of joins among fact & dimension table, I tried to make the dimension tables more independent while they can be connected to fact table with foreign keys rather easily. So most of my dimension tables are more self sustaining and explanatory, which will limit the need for joins between tables to explore out meaningful analytical values from them.

+ **List the steps necessary to pipeline the data into the chosen data model.**

#### 3.1 Conceptual Data Model
I chose a star schema data model for my conceptual data model. Creating a star schema was challenging, seeing the relationship among these tables are rather divergent and disparate. So I did sliced out 3 tables from the `Immigration table` table into `Fact_Immigration_Table (fact table)`, `Individual_Immigrants_Records(dim)` and `Dated_Arrival_Departure (dim)` tables. I created 3 dimension tables from `I94_SAS_Labels_Descriptiontable` table into `US_Port_Code_df (dim)`, `Country_Code_df (dim)`, `US_State_Code (dim)` tables. 

Finally, I created 2 other dimension table `US_City_Temp (dim)` & `US_City_Demog_Race (dim)` tables. So after all these development I have had 6 dimension table and 1 Fact table. The visual correlation among these tables are obvious with this ER diagram [Star-Schema](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/blob/main/DEND_Capstone_Project/Star_Diagram_Dictionary/Immigration_Fact_ER_Star_Schema_Diagram.png). I think according to my data model design for typical query we only need 2/3 joins to find out a result.


#### 3.2 Designing Data Pipelines
`etl.py:` I created my first `Data model to pipeline` with `etl.py` script file to read, explore, transform all the relevant data files and made them immediately available for data analysis. After the needed data-cleaning transformation, I did sent them back to cloud data storage source as a parquet file. By following the data model an analyst can filter, join and extract out needed data and learn valuable insight from them.

`Apache Airflow:` I also used Apache Airflow to automate this data pipeline because of its ease of use; it has ready-to-use operators that can be used to integrate Airflow with Amazon S3-bucket. One of the cool thing of Airflow is it's graphical UI which can be used to monitor and manage data pipeline in real time task flows. To automate the data pipeline processes we can schedule all the task in certain frame and use SLA for efficient follow up management.

### `Step 4:` Run ETL pipeline to Model the Data
---------------------------------------------------------------------------------------------------------------------------------------------------
**4.1** Create the data pipelines and the data model

> The detail step by step processes of the data-model creation is available here with my Jupyter notebook [Final_Capstone_Project_Data_Analysis
](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/blob/main/DEND_Capstone_Project/Final_Capstone_Project_Data_Analysis.ipynb)

**4.2** Include a data dictionary

> My data dictionary for this data model is available [Data Dictionary](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/blob/main/DEND_Capstone_Project/Data_Dictionary.md)

**4.3** Run data quality checks to ensure the pipeline ran as expected

**Data quality checking criteria:**...?
+ Ensuring no empty table after running the ETL data pipeline.
+ Data model should clearly connects each of the dimension table with fact table via a foreign key?
+ Integrity constraints on the relational database (e.g., unique key, data type, etc.)...?

**4.4: Apache Airflow pipeline web UI:**
> My Airfow DAG's takes care of reading data files from Amazon S3 bucket, copying them into empty staging tables, after some transformations of the data-tables load those files back to empty fact and dimension tables. The data quality checks are done with a DAG task. This is a completely automated data pipeline processes done in the cloud environment. Airflow makes the data pipeline testable, maintainable and visual. The detail of Airflow codes are available here [Aiflow pipeline](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/tree/main/DEND_Capstone_Project/Apache_Airflow_Data_Pipeline)

My Air flow data pipeline:![image](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/blob/main/DEND_Capstone_Project/Apache_Airflow_Data_Pipeline/Airflow_Tasks_Flow.png)

### `Step 5:` Project Write Up with feasibility questions:
-------------------------------------------------------------------------------------------------------------------
**5.1: Clearly state the rationale for the choice of tools and technologies for the project.**

+ I used PySpark which is a powerful python based tool to process large amount of data efficiently.
+ Amazon S3 is a highly scalable, secured data storage in the cloud and can be accessed from anywhere in the world. It is very easy to store and retrieve data from S3 storage.
+ I used Apache Airflow to automate, execute, monitor data pipeline tasks as efficiently as needed. 

**5.2: Propose how often the data table should be updated and why.**

+ The I94 `immigration data` and `temperature data` is updated on a monthly basis officially. These data update should follow that guideline.
+ `Demography data` can be updated semi-annually since it's a time conusming, expensive and needs maturing processes.
+ `Temperature data` must be updated daily, because without temperature the data table is useless.
+ `Airport table` can be updated with official guideline.

**5.3: Describe how you would approach the problem differently under the following scenarios:**

**A. `If the data was increased by 100x:`**

Under that kind of scenario I'd run these coding processes on a more powerful computing environment with AWS. For instance, I'd use Amazon EMR (Elastic MapReduce) in a managed cluster platform that simplifies running big data frameworks as such Apache Spark. EMR can process and analyze vast amounts of data and lets user transform and move large amounts of data into and out of other AWS data stores and databases, such as Amazon Simple Storage Service (Amazon S3) and Amazon DynamoDB.

**B. `If the pipelines were run on a daily basis by 7am:`**

Apache Airflow does compatible integration with Python and AWS programming working environment. We can use Apache Airflow with set timeline (in this case around 6:00 am) with each DAG to run the whole ETL data pipeline on  hourly, daily basis or as needed. Also we can use `Service Level Agreement (SLA)` of sending emails in the event a task exceeds its expected time frame from the start of the DAG execution using time delta. Both of these features can be utilized for monitoring the performance of Airflow DAGs.


**C. `If the database needed to be accessed by 100+ people:`**

Amazon Redshift can handle up to 500 connections securely and efficiently with live data sharing within across AWS user accounts. User can easily, instantly and securely query live data from a Redshift cluster with permissions. So we can move this database to Redshift with confidence to handle this kind of needed request. The cost structure should be explored as needed.


### `Step 6:` Analytical query purview:
--------------------------------------------------------------------------------------------------------------------------------------
Here I will share some query and their visual results I peroformed with my notebook and Apache Airflow...?

#### `Query 1:` "Immigrants Visa Type, Visa Purpose, Number of Immigrants & Staying days: ")


#### `Query 2:` "Number of Immigrants by countries: ")

#### `Query 3: Finding immigrant's choice of States to move or visit to?`

#### `Query 4: Immigrants choice city demography >> Total number of immigrant, Foreign-Born & population in US-City-State`

#### `Query 5: Immigrants who didn't leave the country stayed which City`
Answer to these queries can be found here:[Project Notebook]()
----------------------------------------------------------------------------------------
#### Future Improvements:

+ Immigration data set is based at 2016 but temperature data set only get to 2013 which is not enough for us to see the temperature change at 2016.

+ Missing state and city in label description file. This makes it hard to join immigration tables and demography tables.
+ We could have a table with immigrants work...?

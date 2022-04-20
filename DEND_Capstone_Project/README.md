## `Udacity Data Engineering Nanodegree` - `Capstone Project`

### Farhad Md Ahmed

## `Project Summary:`
This is my Capstone project for Udacity Data Engineering Nanodegree program. Here I'd showcase my overall learning consequence from this course. We're given multiple(5) datasets large and medium range in size, our assigned directive is to go through the data exploration phases and extract out relevant valuable insight from these datasets. I personally think, some of these datasets are not directly relevant to our analysis(for instance ..Temperature data), but I'll try to connect out some sort of insight from these exploration.

Our scrutiny on these dataset should help data analysts to understand immigration trend, patterns which in turn help(Govt or Policy makers) to make better decisions on immigration subsequently to help other industry like labor intensive businesses, farming, tourism, construction and many more.

As a data engineer my job would be exploring and developing a final clean data product ready for instant intricate data analysis.

### `project steps:`

`Step` `1:` Scope the Project and Gather Data

`Step` `2:` Explore and Assess the Data

`Step` `3:` Define the Data Model

`Step` `4:` Run ETL to Model the Data

`Step` `5:` Complete Project Write Up

`Step` `6:` Addressing some hypthetical scenario 


### `Step 1:` Scope the Project and Gather Data
With this project I'll create multiple clean data sets ready for immediate analysis to quickly gather insights from the data sets.
The clean data tables will be loaded into S3-bucket or in a Amazon Redshift  as parquet files using PySpark programming language. Those file will then be loaded into Amazon Redshift ......>> using pipeline created using Airflow.

**Describing Data Sets used:**

`I94 Immigration Data:`
The immigration data comes from the [US National Tourism and Trade Office](https://www.trade.gov/national-travel-and-tourism-office). It includes information about people entering the United States with any immigrats year and month, arrival and departure dates, age and year of birth of immigrant, arrival city, port, current residence state, travel mode (air, sea), visa type etc. .....

`World Temperature Data:`
This dataset came from Kaggle. This dataset includes both world and US temperatures. You can read more about it [kaggle](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data).

`U.S. City Demographic Data:`
The demographic data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). It includes demographic information about US cities, with median age, total population, and specific populations (male vs female, foreign-born, different races, etc.). The data is in csv format....

`Airport Code Table:`
This is a simple table of airport detail codes and corresponding cities. It comes from [datahub](https://datahub.io/core/airport-codes#data).

`LABEL description file:`
This is a SAS data type label file icludes column labels used with 'immigration' data sets. Since lot of data-columns came with the immigration dataset in a coded format, this label file will help us create an intelligible values. [data source]()

We'll extract/translate out Country Code, State Code, Port City and Port Codes to match with our immigration data sets.

`A.` `Country Code:` For instance we'll extract out country names corresponding to `i94CIT` &  `i94RES` codes that are found in the immigration data. 

`B.` `State Code:` This maps the `I94ADDR column` in the immigration table.

`C.` `Port City:` We'll find port city name in connection with `i94PORT` where the immigrants came first.

`D.` `Port Code:` The port code will identify the `I94PORT` column in the immigration table. 

`E.` `Visa Codes:` Visa code will decode `i94VISA` column into visa category type issued to an immigrant.

`F.` `Travel Mode:` will show in what transportation mode was used by an immigrant from `I94MODE` column in the immigration table.


### `Step 2:`  Explore and Assess the Data
--------------------------------------------------------------------------------------------------------------
>  Explore the data to identify data quality issues, like missing values, duplicate data, etc. `(Guideline)`
>  
>  Document steps necessary to clean the data `(Guideline)`

I explored those datsets using PySpark programmnig language in Jupyter notebook[link].

#### `DATA-QUALITY` & `CLEANING:`

`A.` **`I94 Immigration Data:` `Data quality issues`:** 
+ Total number of records/rows are 3096313.. and the number of distinct immigration_id's (cicid) was also 3096313 which suggests that the column cicid is unique without any duplicated ID-columns.
+ The data spans arrival dates in the month of April for the year 2016. The dates provided are SAS dates format
+ The arrival & departure dates for all records are in string format not date?
+ There are 7 columns (DTADFILE, VISAPOST, OCCUP, ENTDEPA, ENTDEPD, ENTDEPU, DTADDTO) all are coded with values not clear what they meant?
+ Some of the records have departure dates that is before the arrival date?

**`Cleaning steps:`**
+ Practically I renamed all the columns for clear understanding of the columns-table.
+ Transformed (arrival & departure) SAS dates to PySpark date format.
+ Created 4 different tables(visa category..mode of travel..&)for simplicity of data analysis.
+ Developed extensive dated segmentation of both arrival & departure date columns.
+ Eliminated all unnecessary(7) columns from the table.


`B.`**`GlobalLandTemperaturesByCity.csv:` `data quality issues:`**
+ Number of records with this table: 8,599,212 rows and 8 columns (huge data set).
+ The data > `date` recorded since 18th century(from year `1743` to `2013`)..(wide range issues).
+ We had 364,130 row temperature values were missing out of more than 8 million.
+ We have 159 distinct countries and 3448 city (wide scope of areas).
+ All column values are given string-data type.

**`Cleaning steps:`**
+ I changed the `dt` column to `date` type.
+ Converted `AverageTemperature`, `AverageTemperatureUncertainty` to float-data-type.
+ Converted Year, Day, Month_Num to int and Month_Name to string data type.
+ I chose to limit date access from year `2000` to available.
+ Disposed of all the null-temperature values from the table.
+ Scoped my analytic temperature data limited to USA only.


`C.` **`US-Cities-Demographics.csv` `Data quality issues`:** 
+ Total number of records with this table was 2891 rows and 12 columns.
+ Good news, only 3 columns has null values in range (13 to 16).
+ All the data column-type came with `string-format`
+ Distinct city count 567 and state count is 49
+ 5 distinct Races are embedded with this dataset on 'Race-column'

**`Cleaning steps:`**
+ So I had change columns to appropriate data types (int, float).
+ Pivoted out demographic race data into five different columns. 
+ Chose conversion of upper(City & State) columns for clarity.

`D.` **`Airport Code:` `data quality issues:`**
+ Total number of records is 55075 with 14 columns. 
+ There were only 9189 records have the [iata code](https://airportcodes.io/en/iata-codes/#:~:text=What%20are%20IATA%20codes%3F%20An%20IATA%20code%2C%20consisting,Airport%20has%20the%20%22LHR%22%20as%20the%20IATA%20code.?msclkid=513518aabaac11ec81151a8894006df3) populated.
+ We had 5 columns with large number of null values in it.
+ For instance iata_code has 45886 and local_code column has 26389 number of null values.
+ There are 279 airports was closed.
+ Number of airports appear more than once in the data set.
+ Latitude and longitude are in a single column separated by commas.

**`Cleaning steps:`**
+ I renamed all the column to make them more menaningful.
+ I've removed the airport identification codes(iata_code) those are missing.
+ Excluded `iata_Codes` are came with `zero` values in it.
+ Eliminated airports those are closed and duplicated.
+ Separated longitude and latitude data for clarity, which increased 1 additional column.

`D.` **`I94_SAS_Labels_Description file:` `data quality issues:`**
+ This is a single file with SAS data format came with coded label related to City, State, Port and Country codes.
+ All those columns are decoded with extensive regular expression issue.
+ Data types came with string type.
+ Each columns are mired with extensive regular expression mixed up.

**`Cleaning steps:`**
+ I had to use rigorous regular expression programming steps to clean those code.
+ I used slicing, splitting, stripping & replacing functions to clean codes.
+ I cast some string data type to integer type.
+ Eventually I came up with 3 different data-label columns(Port,Country,State).

### `Step 3:` Define the Data Model
---------------------------------------------------------------------------------------------------------------------
> Map out the conceptual data model and explain why you chose that model (`guideline`)
> 
> List the steps necessary to pipeline the data into the chosen data model (`guideline`)

#### 3.1 Conceptual Data Model
I chose a star schema data model for my conceptual data model. Creating a star schema was challenging, seeing the relationship among these tables are rather divergent and disparate. So I did sliced out 3 dimension tables from the `Immigration table` table into `Fact_Immigration_Inclusive`, `Individual_Immigrants_Records` and `Dated_Arrival_Departure` tables. I created 3 dimension tables from `I94_SAS_Labels_Descriptiontable` table into `US_Port_Code_df`, `Country_Code_df`, `US_State_Code` tables. 

Finally, I created 2 other dimension table `US_City_Temp` & `US_City_Demog_Race` tables. So after all these development I have had 6 dimension table and 1 Fact table. The visual correlation among these tables are obvious but convergence among Fact and dimension tables are efficient while they can be connected indirectly and carefully. So I added multiple query with my [link]() notebook how this star-schema model is efficient.....

For clear visual understanding check out the data model interconnection here: [Data_Model](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/blob/main/DEND_Capstone_Project/Immigration_Fact_ER%20-%20Database%20ER%20diagram.png)

#### 3.2 Designing Data Pipelines
`etl.py:` I created a `Data model to pipeline` with `etl.py` script file to read, transform all the relevant data files made them ready for immediately available for data analysis. After the needed data-cleaning transformation, I did sent them back to cloud data storage source as a parquet file. By following the data model any analysts can filter, join and extract out needed data and learn valuable insight.

`AWS Redshift:` I chose Amazon Redshift to store and analyze data, because redshift can efficient handle large amount of analytical data...

#### `Step 4:` Run ETL to Model the Data
--------------------------------------------------------------------------------------------------------------
**4.1** Create the data pipelines and the data model

The detail step by step processes of the data-model creation steps can be found here with Jupyter notebook [Final_Capstone_Project_Data_Analysis
]()

**4.2** Include a data dictionary

> My data dictionary for this data model is available [Here](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/blob/main/DEND_Capstone_Project/Data_Dictionary.md)

**4.3** Run data quality checks to ensure the pipeline ran as expected

**Data quality checking criteria:**...
+ Ensuring no empty table after running the ETL data pipeline.
+ Data model clearly connects the dimension table with fact table.....
+ Check here.....[Data quality]()
+ Integrity constraints on the relational database (e.g., unique key, data type, etc.)...


#### `Step 5:` Project Write Up with feasibility questions:
-------------------------------------------------------------------------------------------------------------------
What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
Clearly state the rationale for the choice of tools and technologies for the project.

**Document the steps of the process.**

**5.1: Propose how often the data should be updated and why.**
+ The I94 `immigration data` and `temperature data` is updated on a monthly basis officially. These data update should follow that guideline.
+ `Demography data` can be updated semi-annually since it's a time conusming maturing process.
+ We can follow the official guideline in collection, distribution and updating all the datasets.

**5.2: Describe how you would approach the problem differently under the following scenarios:**

**A. If the data was increased by 100x:**

Under that kind of scenario I'd run these coding processes on a more powerful computing environment with AWS. For instance, I'd use Amazon EMR (Elastic MapReduce) in a managed cluster platform that simplifies running big data frameworks as such Apache Spark. EMR can process and analyze vast amounts of data and lets user transform and move large amounts of data into and out of other AWS data stores and databases, such as Amazon Simple Storage Service (Amazon S3) and Amazon DynamoDB.

**B. If the pipelines were run on a daily basis by 7am:**

We can use Apache Airflow with set timeline (in this case around 6:00 am) with each DAG to run the whole ETL data pipeline on  hourly, daily basis or as needed. Apache Airflow does compatible integration with Python and AWS programming working environment. Also we can use Service Level Agreement (SLA) of sending emails in the event a task exceeds its expected time frame from the start of the DAG execution using time delta. These entries can be utilized for monitoring the performance of both the Airflow DAGs.


**C. If the database needed to be accessed by 100+ people:**

Amazon Redshift can handle up to 500 connections securely and efficiently with live data sharing within across AWS user accounts. User can easily, instantly and securely query live data from a Redshift cluster with permissions. So we can move this database to Redshift with confidence to handle this kind of needed request. The cost structure should be explored as needed.

----------------------------------------------------------------------------------------
Future Improvements
There are several incompletions within these data sets. We will need to collect more data to get a more complete SSOT database.

Immigration data set is based at 2016 but temperature data set only get to 2013 which is not enough for us to see the temperature change at 2016.

Missing state and city in label description file. This makes it hard to join immigration tables and demography tables.

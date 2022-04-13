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
 + Explore the data to identify data quality issues, like missing values, duplicate data, etc. `(Guideline)`
 + Document steps necessary to clean the data `(Guideline)`

I explored those datsets using PySpark programmnig language in Jupyter notebook[link].

#### `DATA-QUALITY` & `CLEANING:`

`A.` **`I94 Immigration Data:` `Data quality issues`:** 
+ Total number of records/rows are 3096313.. and the number of distinct immigration_id's (cicid) was also 3096313 which suggests that the column cicid is unique.
+ The data spans arrival dates in the month of April for the year 2016. The dates provided are SAS dates format
+ The arrival date is populated for all records.... 
+ There are 8 columns all are coded with values not clear what they meant
+ Some of the records have departure dates that is before the arrival date. So I had to transform the SAS date to pyspark date format to make it intelligible.

**`Cleaning steps:`**
+ Practically renamed all the columns for clarity
+ Transforming SAS date to PySpark date format
+ Created 4 different tables(...) for simplicity of analysis
+ Extensive dated segmentation of both arrival & departure columns


`B.`**`GlobalLandTemperaturesByCity.csv:` `data quality issues:`**
+ Number of records with this table: 8,599,212 and columns: 8
+ The data > date recorded since 18th century(from year `1743` to `2013`)
+ We only had 364,130 temperature data was missing out of more than 8 million
+ We have 159 distinct countries and 3448 city
+ Columns dt, AverageTemperature, aAverageTemperatureUncertainty, Latitude, Longitude all are string data type

**`Cleaning steps:`**
+ I changed the `dt` column to `date` type
+ Columns (`AverageTemperature`, `AverageTemperatureUncertainty`, `Latitude`, `Longitude`) converted to `integer` data type
+ I chose to limit data date access from year `2000` to available.
+ Disposed of all the null-temperature values from the table.
+ Selected temperature data only for USA.


`C.` **`US-Cities-Demographics.csv` `Data quality issues`:** 
+ Total number of records with this table was 2891 and 12 columns
+ Only 3 columns has null values in range (13 to 16)
+ Distinct city count 567 and state count is 49
+ 5 distinct Races embedded with this dataset on 'Race-column'
+ All the data column-type came with `string-format`

**`Cleaning steps:`**
+ So I had change them to appropriate data types (int,float).
+ There were some(5) distinct demographic race present in the race column. 
+ I did separate them with a pivot table

`D.` **`Airport Code:` `data quality issues:`**
+ Total number of records is 55075 with 14 columns 
+ There were only 9189 records have the [iata code](https://airportcodes.io/en/iata-codes/#:~:text=What%20are%20IATA%20codes%3F%20An%20IATA%20code%2C%20consisting,Airport%20has%20the%20%22LHR%22%20as%20the%20IATA%20code.?msclkid=513518aabaac11ec81151a8894006df3) populated
+ There are 279 airports was closed
+ Number of airports appear more than once in the data set
+ Latitude and longitude are in a single column separated by commas

**`Cleaning steps:`**
+ I've removed the airport identification codes(iata_code) those are missing
+ Excluded `iata_Codes` are came with `zero` values in it
+ Eliminated airports those are closed and duplicated
+ Separated longitude and latitude data for clarity

`D.` **`I94_SAS_Labels_Description file:` `data quality issues:`**
+ There are 8 columns came coded with immigration file
+ All those columns are decoded with extensive regular expression issued
+ Data types came with string type

**`Cleaning steps:`**
+ I had to use rigorous regular expression programming steps to clean those code
+ I used slicing, splitting, stripping & replacing functions to clean codes
+ I casted some string data type to integer type



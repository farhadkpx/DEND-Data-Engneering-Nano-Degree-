## `Udacity Data Engineering Nanodegree` - `Capstone Project`
----------------------------------------------------------
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

`Global and U.S. City Demographic Data:`
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



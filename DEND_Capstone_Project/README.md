## `Udacity Data Engineering Nanodegree` - `Capstone Project`
----------------------------------------------------------
### Farhad Md Ahmed

## `Project Summary:`
This is my Capstone project for Udacity Data Engineering Nanodegree program. Here I'd showcase my overall learning results from this course. We're given multiple(5) datasets large and medium range in size, our assigned directive is to go through the data exploration phases and extract out relevant valuable insight from these datasets. I personally think, some of these datasets are not directly relevant to our analysis(for instance ..Temperature data), but I'll try to connect out some sort of insight from these exploration.

Our scrutiny on these dataset should help data analysts to understand immigration trend, patterns which in turn help(Govt or Policy makers) to make better decisions on immigration subsequently to help other industry like labor intensive businesses, farming, construction and many more.

As a data engineer my job would be exploring and developing a final clean data product ready for instant intricate data analysis.

### `project steps:`

`Step` `1:` Scope the Project and Gather Data

`Step` `2:` Explore and Assess the Data

`Step` `3:` Define the Data Model

`Step` `4:` Run ETL to Model the Data

`Step` `5:` Complete Project Write Up

`Step` `6:` Addressing some hypthetical scenario 


### Step 1: Scope the Project and Gather Data
In this project an analytical database will be made available for the USA government, so they can quickly gather insights from the data.

At a high level,
The data is cleaned and loaded into S3 as parquet files using Spark. This is then loaded into Redshift using pipeline created using Airflow.

Describe and Gather Data
Data Sets Used:
The following data was used to build the datawarehouse:
I94 Immigration Data:
The immigration data comes from the US National Tourism and Trade Office. It includes information about people entering the United States, such as immigration year and month, arrival and departure dates, age and year of birth of immigrant, arrival city, port, current residence state, travel mode (air, sea), visa type etc. Specificially, the data from April 2016 is used to showcase this project, which is over 1 million records. The data is in parquet format.

U.S. City Demographic Data:
The demographic data comes from OpenSoft. It includes demographic information about US cities, such as the median age, total population, and specific populations (male vs female, foreign-born, different races, etc.). The data is in csv format.

Country Data
This data was provided in I94_SAS_Labels_Descriptions.SAS in the provided project and contains a mapping of country names and their I94 codes that are found in the immigration data. I used Spark to create a parquet file with this information.

Visa Codes
This data was provided in I94_SAS_Labels_Descriptions.SAS in the provided project. This maps to the I94VISA column in the immigration table. I used Spark to create a parquet file with this information.

State Code
This data was provided in I94_SAS_Labels_Descriptions.SAS in the provided project. This maps to the I94ADDR column in the immigration table. I used Spark to create a parquet file with this information.

Travel Mode
This data was provided in I94_SAS_Labels_Descriptions.SAS in the provided project. This maps to the I94MODE column in the immigration table. I used Spark to create a parquet file with this information.

Port Code
This data was provided in I94_SAS_Labels_Descriptions.SAS in the provided project. This maps to the I94PORT column in the immigration table. I used Spark to create a parquet file with this information.

Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes

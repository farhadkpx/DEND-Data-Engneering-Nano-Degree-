
| Fact_Immigration_Inclusive  |                |   FACT TABLE                                                           |
------------------------------|----------------|------------------------------------------------------------------------|
| **Column Name**             | **Data Type**  | **Description**                                                        |
| `Immigration_Id`            | integer        | **`Primary key`**                                                      |
| Admission_Num               | integer        | (FK) 'Dated_Arrival_Departure' & individual immigrants record table    |
| Citizenship_Country_Code    | integer        | (FK) to `Country_Codes_df` table on immigrant citizenship country code |
| Residency_Country_Code      | integer        | (FK) to `Country_Codes_df` table on immigrant residency_country_code   |
| Current_State_Code          | string         | (FK) to `State_Codes_df` based on current state_code of immigrants     |
| Visa_Type                   | string         | Official visa type category of immigrants assigned by the department   |
| Visa_Category               | string         | Definition of visa code type category                                  |
| Immigration_Year            | integer        | The year an immigrant entered the country                              |
| Immigration_Month           | integer        | The month an immigrant entered the country                             |
| Port_Code                   | string         | (FK) to `Port_locations_df` where immigrants arrived at a US port      |
| Arrival_Date                | string         | The arrival_date of an immigrant in USA                                |
| Departure_Date              | string         | The departure_date of an immigrant from USA                            |
| Immigrants_Age              | integer        | Age of an immigrant                                                    |   
| Match_Flag                  | string         | FlagString to show match of an immigrants arrival and departure records|
| Birth_Year                  | integer        | Birth Year of an immigrant                                             |
| Gender                      | string         | Official gender of an immigrant                                        |
| Airline_Code                | string         | Airline with which immigrants arrived in the US                        |
| Flight_Num                  | string         | The flight number of an incoming immigrants                            |
| Means_of_Travel             | string         | The physical way an immigrant chose to enter the country               |

---------------------------------------------------------------------------------------------------------------------------------
| Individual_Immigration_Records |                |   DIMENSION TABLE                                                   |
------------------------------|----------------|------------------------------------------------------------------------|
| **Column Name**             | **Data Type**  | **Description**                                                        |
| `Admission_Num`             | integer        | **`Primary key`**                                                      |
| Immigration_Id              | integer        | (FK) the matching primary key of `Fact-Immigration` table              |
| Arrival_Date                | string         | The arrival_date of an immigrant in USA                                |       
| Citizenship_Country_Code    | integer        | Foreign key to dim_country based on immigrant citizenship country code |
| Gender                      | string         | Official gender of an immigrant                                        |
| Immigrants_Age              | integer        | Age of an immigrant                                                    |  
| Departure_Date              | string         | The departure_date of an immigrant from USA                            |
| Visa_Type                   | string         | Official visa type category of immigrants assigned by the department   |
| Match_Flag                  | string         | FlagString to show match of an immigrants arrival and departure records|


| Dated_Arrival_Departure     |                |   DIMENSION TABLE                                                      |
|----------------------------------------------|------------|-----------------------------------------------------------|
| **Column Name**             | **Data Type**  | **Description**                                                        |
| `Admission_Num`             | bigint         | **`Primary key`**                                                      |      
| Arrival_Date                | string         | The Arrival_Date of an immigrant in existing string format             |
| Residency_Country_Code      | integer        | The country code of an incoming                    immigrant           |
| Arrival_Year                | integer        | Arrival year in the 'yyyy' format, for instance : 2016                 |
| Arrival_Month               | string         | Arrival month in the 'MMMM' format such: January-December              |
| Arrival_Day                 | integer        | Arrival day in the 'dd' format ranges: 1 to 31                         |
| Departure_Date              | string         | The Departure_Date of an immigrant in existing string format           |
| Depart_Year                 | integer        | Departure year in the 'yyyy' format, for instance : 2017               |
| Depart_Month                | string         | Departure month in the 'MMMM' format such: January-December            |
| Depart_Day                  | integer        | Departure day in the 'dd' format ranges: 1 to 31                       |
| Visa_Type                   | string         | Official type of visa issued                                           |
| Port_Code                   | string         | Port code where immigrants entered the country                         |



------------------------------------------------------------------------------------------------------------------------------------
 
 | **Immigration_Demog_Race**                  |                |      **DIMENSION TABLE**                                |
 ----------------------------------------------|----------------|---------------------------------------------------------|
 | **Column Name**                             | **Data Type**  | **Description**                                         |
 | `Immigration_Id`                            | integer        | **`primary key`**                                       |
 | Average_Household_Size                      | float          | Average household size in US citY                       |
 | Male_Population                             | integer        | male population in US city                              |
 | Gender                                      | string         | Sex of the immigrants                                   |
 | Female_Population                           | integer        | female population in US city                            |
 | Median_Age                                  | float          | median age of population of city                        |
 | State                                       | string         | US State name                                           |                 
 | American Indian and Alaska Native           | long           | American Indian and Alaska Native in the city           |
 | Asian                                       | long           | Asian populatin in US cities                            |
 | Black or African-American                   | long           | counting Black or African-American population           |
 | hispanic_or_latino_population               | long           | Hispanic or Latino population in the city               |
 | white                                       | long           | White population  in US city                            |   


-------------------------------------------------------------------------------------------------------------------------------------

| **US_Cities_Temperature** |                    |     DIMENSION (SUPPORTIVE) TABLE                    |
|---------------------------|--------------------|-----------------------------------------------------|
| **Column Name**           | **Data Type**      | **Description**                                     |
| `City`                    | string             | **`primary key`** (City name)                       |
| Country                   | string             | Name of the country                                 |
| Date_Records              | date               | Date when temperature recorded                      |
| Year                      | string             | Year in 'yyyy' format as temperature recorded       |
| Month_Name                | string             | Month_Name in 'MMMM' format as temperature recorded |
| Month_Num                 | int                | Month_Num in 'MM' format as recorded                |
| Day                       | string             | Day in 'dd' format as recorded                      |




---------------------------------------------------------------------------------------------------------------------------------------
| **Port_locations_df**     |                |     DIMENSION TABLE                                   |
|---------------------------|----------------|-------------------------------------------------------|
| **Column Name**           | **Data Type**  | **Description**                                       |
| `Port_Code`               | string         | **`primary key`**, immigrants (i94 port code)         |
|  Port_City                | string         | immigrants i94 port entrance city                     |
|  Port_State               | string         | immigrants i94 port entrance state                    |


| **Country_Codes_df**      |                |      DIMENSION TABLE                                  |
|---------------------------|----------------|-------------------------------------------------------|
| **Column Name**           | **Data Type**  | **Column Description**                                |
| `Country_Codes`           | integer        | **`primary key`** (i94 country code)                  |
|  Country_Names            | string         | immigrants  (i94 country) name                        |



| **State_Codes_df**        |                |    DIMENSION TABLE                                    |
|---------------------------|----------------|-------------------------------------------------------|
| **Column Name**           | **Data Type**  | **Column Description**                                |
| `State_Codes`             | string         | **`primary key`** immigrants (i94 state code)         |
| State_Names               | string         | immigrants i94 state name                             |



         





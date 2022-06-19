
| **Fact_Immigration_Table**  |                |   **FACT TABLE**                                                          |
------------------------------|----------------|---------------------------------------------------------------------------|
| **Column Name**             | **Data Type**  | **Description**                                                           |
| `Immigration_Id`            | `integer`      | **`Primary key`**  Immigrants unique identifier                           |
| `Admission_Num`             | `bigint`       | (FK) to `Individual_Immigrants_Records` table & `dated_arrival` table     |
| `Citizenship_Country`       | `integer`      | (FK) to `Country_Codes_df` table on immigrant's citizenship country code  |
| `Residency_Country`         | `integer`      | (FK) to `Country_Codes_df` table on immigrant residency_country_code      |
| `Current_State`             | `string`       | (FK) to `State_Codes_df` based on current immigrants current state code   |
| Visa_Type                   | string         | Official visa type category of immigrants assigned by the department      |
| Visa_Purpose                | string         | What reason visa was issued to an immigrants or toursts                   |
| Immigration_Year            | integer        | The year an immigrant entered the country                                 |
| Immigration_Month           | integer        | The month an immigrant entered the country                                |
| `Port_Code`                 | `string`       | `(FK)` to `Port_locations`, `US_City_Temp`, `US_Demog_Race` tables        |
| `Arrival_Date`              | `date`         | `(FK)` to `dim_dated_arrival_departure` table as `Entry_Date`             |
| Departure_Date              | date           | The departure_date of an immigrant from USA                               |
| Immigrants_Age              | integer        | Age of an immigrant                                                       |   
| Match_Flag                  | string         | FlagString to show match of an immigrants arrival and departure records   |
| Birth_Year                  | integer        | Birth Year of an immigrant                                                |
| Gender                      | string         | Official gender of an immigrant                                           |
| Airline_Code                | string         | Airline code with which immigrants arrived in the US                      |
| Flight_Num                  | string         | The Airline flight number of an incoming immigrants                       |
| Means_of_Travel             | string         | The physical means of transportation of an  incoming immigrants           |



----------------------------------------------------------------------------------------------------------------------------|
| **dim_Individual_Immigrants_Records** |      |   **DIMENSION TABLE**                                                      |
------------------------------|----------------|----------------------------------------------------------------------------|
| **Column Name**             | **Data Type**  | **Description**                                                            |
| `Entry_Num`                 | `bigint`       | **`Primary key`** The official unique admission number of an immigrant     |
| `Immigration_Id`            | integer        | `(FK)` the matching primary key of `Fact-Immigration` table                |
| `Arrival_Date`              | `date`         | The arrival_date of an immigrant in USA                                    |       
| Citizenship_Country         | integer        | Immigrants citizenship country code                                        |
| Gender                      | string         | Official gender of an immigrant                                            |
| Immigrants_Age              | integer        | Age of an immigrant                                                        |  
| Departure_Date              | date           | The departure_date of an immigrant from USA                                |
| Visa_Type                   | string         | Official visa type category of immigrants assigned by the department       |
| Match_Flag                  | string         | FlagString to show match of an immigrants arrival and departure records    |


| **dim_Dated_Arrival_Departure** |            |   **DIMENSION TABLE**                                                      |
|-----------------------------|----------------|----------------------------------------------------------------------------|
| **Column Name**             | **Data Type**  | **Description**                                                            |
| `Entry_Date`                | `date`         | **`Primary key`** The Arrival_Date of an immigrant                         |
| `Admission_Num`             | `bigint`       | `(FK)` in `fact immigration_table`                                         |      
| Citizenship_Country         | integer        | The country code of an incoming immigrant                                  |
| Arrival_Year                | integer        | Arrival year in the 'yyyy' format, for instance : 2016                     |
| Arrival_Month               | string         | Arrival month in the 'MMMM' format such: January-December                  |
| Arrival_Day                 | integer        | Arrival day in the 'dd' format ranges: 1 to 31                             |
| Departure_Date              | date           | The Departure_Date of an immigrant in existing string format               |
| Depart_Year                 | integer        | Departure year in the 'yyyy' format, for instance : 2017                   |
| Depart_Month                | string         | Departure month in the 'MMMM' format such: January-December                |
| Depart_Day                  | integer        | Departure day in the 'dd' format ranges: 1 to 31                           |
| Visa_Type                   | string         | Official type of visa issued                                               |
| `Port_Code`                 | `string`       | Port code with Fact_Immigrantion table                                     |



------------------------------------------------------------------------------------------------------------------------------------

| **dim_US_Cities_Tempr**   |                    |     **DIMENSION TABLE**                                                  |
|---------------------------|--------------------|--------------------------------------------------------------------------|
| **Column Name**           | **Data Type**      | **Description**                                                          |
| `Date_Records`            | `date`             | Official date when temperature was recorded                              |  
| Avg_Temp                  | float              | The recorded average temperature of a City on a specific date            |                       
| `US_City`                 | `string`           | City name where temperature was recorded                                 |
| Country                   | string             | Name of the country, City is located                                     |
| Year                      | integer            | Year in 'yyyy' date format recorded                                      |
| Month_Name                | string             | Month_Name in 'MMMM' format recorded                                     |
| Month_Num                 | integer            | Month_Num in 'MM' format recorded                                        |
| Day                       | integer            | Day in 'dd' format recorded                                              |
| `City_Port`               | `string`           | **`primary key`** Port code where the immigrant entered in USA           |
| `Port_State`              | `string`           | State code where US Port & City is located                               |


 | **dim_US_City_Demog_Race**                  |                |      **DIMENSION TABLE**                                  |
 ----------------------------------------------|----------------|-----------------------------------------------------------|
 | **Column Name**                             | **Data Type**  | **Description**                                           |
 | `Demog_City`                                | `string`       | The city where demography data was recorded               |
 | State_Name                                  | string         | Name of the state where City is located                   |
 | Average_Household_Size                      | float          | Average household size in the US city                     |
 | Male_Population                             | integer        | Male population in US city                                |
 | Female_Population                           | integer        | Female population in US city                              |
 | Total_Population                            | integer        | Total population of the designated city                   |
 | Median_Age                                  | float          | Median age of the city population                         |
 | Number_of_Veterans                          | int            | The number of veteran population of the city              |
 | American_Indian_and_Alaska_Native           | int            | American Indian and Alaska Native in the city             |
 | Asian_Population                            | int            | Asian populatin in US cities                              |
 | Black_or_African_American                   | int            | counting Black or African-American population             |
 | hispanic_or_latino_population               | int            | Hispanic or Latino population in the city                 |
 | white_Population                            | int            | White population  in US city                              |  
 | `Demog_Port`                                | `string`       | **`primary key`** Immigrants arrival Port code            |
 | `Port_State`                                | `string`       | State code where US Port & City is located                |


---------------------------------------------------------------------------------------------------------------------------------------
| **Port_locations_df**     |                |     **DIMENSION TABLE**                                                      |
|---------------------------|----------------|------------------------------------------------------------------------------|
| **Column Name**           | **Data Type**  | **Description**                                                              |
| `Port_Code`               | `string`       | **`primary key`**, immigrants (i94 port code)                                |
|  Port_City                | `string`       | `(FK)` with Temp_table & Demography table                                    |
|  Port_State               | `string`       | `(FK)` Immigration & Demography table & entrance State                       |


| **Country_Codes_df**      |                |      **DIMENSION TABLE**                                                     |
|---------------------------|----------------|------------------------------------------------------------------------------|
| **Column Name**           | **Data Type**  | **Column Description**                                                       |
| `Country_Codes`           | `integer`      | **`primary key`** immigrants (i94 country code)                              |
|  Country_Names            | string         | immigrants  (i94 country) names                                              |


| **State_Codes_df**        |                |    DIMENSION TABLE                                                           |
|---------------------------|----------------|------------------------------------------------------------------------------|
| **Column Name**           | **Data Type**  | **Column Description**                                                       |
| `State_Codes`             | `string`       | **`primary key`** immigrants (i94 state code)                                |
| State_Names               | string         | immigrants (i94 state) name                                                  |



         








| Fact_Immigration_Detail     |                |   FACT TABLE                                                           |
------------------------------|----------------|------------------------------------------------------------------------|
| **Column Name**             | **Data Type**  | **Description**                                                        |
| `Immigration_Id`            | **integer**    | **Primary key**                                                        |
| Admission_Num               | integer        | Foreign key to 'Dated_Arrival_Departure' table                         |
| Citizenship_Country_Code    | integer        | Foreign key to dim_country based on immigrant citizenship country code |
| Residency_Country_Code      | integer        | Foreign key to dim_country based on immigrant residency_country_code   |
| Current_State_Code          | string         | Foreign key to dim_state based on current state_code of immigrant      |
| Visa_Type                   | string         | Official visa type category of immigrants assigned by the department   |
| Visa_Code                   | integer        | The inteneded purpose of the visa was issued to an immigrant applicant |
| Visa_Category               | string         | Definition of visa code type category                                  |
| Travel_Mode                 | integer        | Numerical definition of immigrants choice of transporation entrance    |
| Immigration_Year            | integer        | The year an immigrant entered the country                              |
| Immigration_Month           | integer        | The month an immigrant entered the country                             |
| Port_Code                   | string         | Foreign key to dim_ports based on immigrant arrival at US port         |
| Arrival_Date                | string         | The arrival_date of an immigrant in USA                                |
| Departure_Date              | string         | The departure_date of an immigrant from USA                            |
| Age                         | integer        | Age of an immigrant                                                    |   
| Match_Flag                  | string         | FlagString to show match of an immigrants arrival and departure records|
| Birth_Year                  | integer        | Birth Year of an immigrant                                             |
| Gender                      | string         | Official gender of an immigrant                                        |
| Airline_Code                | string         | Airline with which immigrants arrived in the US                        |
| Flight_Num                  | string         | The flight number of an incoming immigrants                            |
| Means_of_Travel             | string         | The physical way an immigrant chose to enter the country               |




| Dated_Arrival_Departure     |                |   DIMENSION TABLE                                                      |
|----------------------------------------------|------------|-----------------------------------------------------------|
| **Column Name**             | **Data Type**  | **Description**                                                        |
| `Admission_Num`             | **bigint**     | **Primary key**                                                        |      
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



 
 | dim_city                                    |            |                                                           |
 ----------------------------------------------|------------|---------------------------------------------------------  |
 | **Column Name**                             | **Data Type**  | **Description**                                     
 | city_id                                     | integer    | primary key                                                |
 | city                                        | string     | city name                                               |
 | state_id                                    | integer    | Foreign key to dim_state                                |
 | median_age                                  | float      | median age of population of city                        |
 | male_population                             | integer    | male population in city                                 |
 | female_population                           | integer    | female population in city                               |
 | total_population                            | integer    | total population in city                                |
 | number_of_veterans                          | integer    | number of veterans in city                              |
 | foreign_born                                | integer    | foreign_born population in city                         |
 | average_household_size                      | float      | Average household size in city                          |
 | americanindian_and_alaskannative_population | integer    | Population of AmericanIndian and AlaskanNatives in city |
 | asian_population                            | integer    | Asian population in city                                |
 | black_or_africanamerican_population         | integer    | Black or AfricanAmerican population  in city            |
 | hispanic_or_latino_population               | integer    | Hispanic or Latino population in city                   |
 | white_population                            | integer    | White population  in city                               |   


-------------------------------------------------------------------------------------------------------------------------------------

| **US_Cities_Temperature** |                    |     DIMENSION TABLE                                 |
|---------------------------|--------------------|-----------------------------------------------------|
| **Column Name**           | **Data Type**      | **Description**                                     |
| `City`                    | string             | `primary key` (City name)                           |
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
| `Port_Code`               | string         | `primary key`, immigrants (i94 port code)             |
|  Port_City                | string         | immigrants i94 port entrance city                     |
|  Port_State               | string         | immigrants i94 port entrance state                    |


| **Country_Codes_df**      |                |      DIMENSION TABLE                                  |
|---------------------------|----------------|-------------------------------------------------------|
| **Column Name**           | **Data Type**  | **Column Description**                                |
| `Country_Codes`           | integer        | `primary key` (i94 country code)                      |
|  Country_Names            | string         | immigrants  (i94 country) name                        |



| **State_Codes_df**        |                |    DIMENSION TABLE                                    |
|---------------------------|----------------|-------------------------------------------------------|
| **Column Name**           | **Data Type**  | **Column Description**                                |
| `State_Codes`             | string         | `primary key` immigrants (i94 state code)             |
| State_Names               | string         | immigrants i94 state name                             |



         






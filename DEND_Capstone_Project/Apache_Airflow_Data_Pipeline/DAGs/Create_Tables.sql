
CREATE TABLE IF NOT EXISTS  public.Staging_Immigration_Fact_Table (
	 Immigration_Id              int8 NOT NULL,
     Citizenship_Country         int4,
     Residency_Country           int4,
     Current_State               varchar(20),
     Visa_Type                   varchar(25),
     Immigration_Year            int4,
     Immigration_Month           int4,
     Port_Code                   varchar(25),
     Arrival_Date                date,
     Departure_Date              date,
     Immigrants_Age              int4,
     Match_Flag                  varchar(5),
     Birth_Year                  int4,
     Gender                      varchar(10),
     Airline_Code                varchar(15),
     Admission_Num               int8 NOT NULL,
     Flight_Num                  varchar(15),
     Means_of_Travel             varchar(20),
     Visa_Purpose                varchar(25)
);



CREATE TABLE IF NOT EXISTS  public.Fact_Immigration_Table (
	 Immigration_Id              int8 NOT NULL,
     Citizenship_Country         int4,
     Residency_Country           int4,
     Current_State               varchar(20),
     Visa_Type                   varchar(25),
     Immigration_Year            int4,
     Immigration_Month           int4,
     Port_Code                   varchar(25),
     Arrival_Date                date,
     Departure_Date              date,
     Immigrants_Age              int4,
     Match_Flag                  varchar(5),
     Birth_Year                  int4,
     Gender                      varchar(10),
     Airline_Code                varchar(15),
     Admission_Num               int8 NOT NULL,
     Flight_Num                  varchar(15),
     Means_of_Travel             varchar(20),
     Visa_Purpose                varchar(25),
     CONSTRAINT pkey_Immigration_Id  PRIMARY KEY(Immigration_Id)
   
);    



CREATE TABLE IF NOT EXISTS public.Staging_Individual_Immigrants_Records(
     Entry_Num                     int8,
     Immigration_Id                int8,
     Arrival_Date                  date,
     Citizenship_Country           int4,
     Immigrants_Age                int4,
     Gender                        varchar(5),
     Departure_Date                date,
     Visa_Type                     varchar(25),
     Match_Flag                    varchar(5)
 );
 
 
 CREATE TABLE IF NOT EXISTS public.dim_Individual_Immigrants_Records(
     Entry_Num                     int8,
     Immigration_Id                int8,
     Arrival_Date                  date,
     Citizenship_Country           int4,
     Immigrants_Age                int4,
     Gender                        varchar(5),
     Departure_Date                date,
     Visa_Type                     varchar(25),
     Match_Flag                    varchar(5),
     CONSTRAINT pkey_Entry_Num  PRIMARY KEY(Entry_Num)
);    

 
  

CREATE TABLE IF NOT EXISTS public.Staging_Dated_Arrival_Departure(
     Entry_Date                    date NOT NULL,
     Admission_Num                 int8,
     Citizenship_Country           int4,
     Arrival_Year                  int4,
     Arrival_Month                 varchar(15),
     Arrival_Day                   int4,
     Departure_Date                date,
     Depart_Year                   int4,
     Depart_Month                  varchar(15),
     Depart_Day                    int4,
     Visa_Type                     varchar(25),
     Port_Code                     varchar(10)
);

CREATE TABLE IF NOT EXISTS public.dim_Dated_Arrival_Departure(
     Entry_Date                    date NOT NULL,
     Admission_Num                 int8,
     Citizenship_Country           int4,
     Arrival_Year                  int4,
     Arrival_Month                 varchar(15),
     Arrival_Day                   int4,
     Departure_Date                date,
     Depart_Year                   int4,
     Depart_Month                  varchar(15),
     Depart_Day                    int4,
     Visa_Type                     varchar(25),
     Port_Code                     varchar(10),
     CONSTRAINT pkey_Entry_Date  PRIMARY KEY(Entry_Date)
  );  
  




CREATE TABLE  IF NOT EXISTS public.Staging_Country_Codes (
	Country_Codes int4,
	Country_Names varchar(100)
);

CREATE TABLE  IF NOT EXISTS public.dim_Country_Codes (
	Country_Codes int4,
	Country_Names varchar(100),
    CONSTRAINT pkey_Country_Codes PRIMARY KEY (Country_Codes)
   
);


CREATE TABLE  IF NOT EXISTS public.Staging_State_Codes (
	State_Codes varchar(25) NOT NULL,
	State_Names varchar(25)
);

CREATE TABLE  IF NOT EXISTS public.dim_State_Codes (
	State_Codes varchar(25) NOT NULL,
	State_Names varchar(25),
    CONSTRAINT pkey_State_Codes PRIMARY KEY (State_Codes)
    
);



CREATE TABLE  IF NOT EXISTS public.Staging_Port_Locations (
	Port_Codes varchar(25) NOT NULL,
	Port_Citys varchar(50),
    Port_States varchar(50)
);

CREATE TABLE  IF NOT EXISTS public.dim_Port_Locations (
	Port_Codes    varchar(25) NOT NULL,
	Port_Citys    varchar(50), 
    Port_States    varchar(50),
    CONSTRAINT pkey_Port_Codes PRIMARY KEY (Port_Codes)
);




CREATE TABLE IF NOT EXISTS public.Staging_US_City_Temperature (
	Date_Records    date NOT NULL,
    Year            int4,    
	Month_Name      varchar(20),
	Month_Num       int4,
    Day             int4,
    Avg_Temp        DOUBLE PRECISION,
    US_City         varchar(50),
    Country         varchar(30),
    City_Port       varchar(15),
    Port_States     varchar(25)
);


CREATE TABLE IF NOT EXISTS public.dim_US_City_Temperature (
	Date_Records    date NOT NULL,
    Year            int4,    
	Month_Name      varchar(20),
	Month_Num       int4,
    Day             int4,
    Avg_Temp        DOUBLE PRECISION,
    US_City         varchar(50),
    Country         varchar(30),
    City_Port       varchar(15),
    Port_States     varchar(25),
	CONSTRAINT pkey_City_Port PRIMARY KEY (City_Port)
 );
 
 


CREATE TABLE IF NOT EXISTS public.Staging_US_City_Demog_Race (
    Demog_City                varchar(50) NOT NULL,
    State_Name                varchar(25),
    Median_Age                DOUBLE PRECISION,
    Male_Population           int8,
    Female_Population         int4,
    Total_Population          int4,
    Number_Of_Veterans        int4,
    Foreign_Born              int4,
    Average_Household_Size    DOUBLE PRECISION,
    American_Indian_Alaska_Native   int8,
    Asian_Population          int8,
    Black_African_American    int8,
    Hispanic_Latino           int8,
    White_Population          int8,
    Demog_Port                varchar(50),
    Port_States               varchar(50)
);


CREATE TABLE IF NOT EXISTS public.dim_US_City_Demog_Race (
    Demog_City                varchar(50) NOT NULL,
    State_Name                varchar(25),
    Median_Age                DOUBLE PRECISION,
    Male_Population           int8,
    Female_Population         int4,
    Total_Population          int4,
    Number_Of_Veterans        int4,
    Foreign_Born              int4,
    Average_Household_Size    DOUBLE PRECISION,
    American_Indian_Alaska_Native   int8,
    Asian_Population          int8,
    Black_African_American    int8,
    Hispanic_Latino           int8,
    White_Population          int8,
    Demog_Port                varchar(50),
    Port_States               varchar(50),                         
    CONSTRAINT  pkey_Demog_Port PRIMARY KEY (Demog_Port)
    
);
    


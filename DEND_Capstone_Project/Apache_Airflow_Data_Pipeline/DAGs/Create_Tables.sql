
##=================== PORT + STATE + COUNTRY ========== STAGING & DIMENSION ==============

#---------------STAGING & DIMENSION COUNTRY CODES-----------------------------
CREATE TABLE  IF NOT EXISTS public.Staging_Country_Codes (
	Country_Codes int4 NOT NULL,
	Country_Names varchar(25),
);


CREATE TABLE  IF NOT EXISTS public.dim_Country_Codes (
	Country_Codes int4 NOT NULL,
	Country_Names varchar(25),
    CONSTRAINT pkey_Country_Codes PRIMARY KEY (Country_Codes)
  
);

#---------------STAGING & DIMENSION STATE CODES-----------------------------
CREATE TABLE  IF NOT EXISTS public.Staging_State_Codes (
	State_Codes varchar(25) NOT NULL,
	State_Names varchar(25)
);


CREATE TABLE  IF NOT EXISTS public.dim_State_Codes (
	State_Codes varchar(25) NOT NULL,
	State_Names varchar(25),
    CONSTRAINT pkey_State_Codes PRIMARY KEY (State_Codes)
  
);


#-------------- STAGING & DIMENSION PORT LOCATIONS -----------------------------------------
CREATE TABLE  IF NOT EXISTS public.Staging_Port_Locations (
	Port_Codes varchar(25) NOT NULL,
	Port_Citys varchar(25),
    Port_States varchar(25)
);


CREATE TABLE  IF NOT EXISTS public.dim_Port_Locations (
	Port_Codes    varchar(256) NOT NULL,
	Port_Citys    varchar(256), 
    Port_States    varchar(256),
    CONSTRAINT pkey_Port_Codes PRIMARY KEY (Port_Codes)
);

#====================== CITY TEMPERATURE ========== STAGING & DIMENSION ==============================



#----------------US CITY TEMPERATURE-----------------------------------
CREATE TABLE IF NOT EXISTS public.Staging_US_City_Temperature (
	Date_Records    date NOT NULL,
	US_City         varchar(50),
	Country         varchar(30),
	Year            int4,
	Month_Name      varchar(25),
	Month_Num       int4,
    Day             int4,
    Avg_Temp        DOUBLE PRECISION,../float,
    US_Port         varchar(15),
    Port_State      varchar(25)
);


CREATE TABLE IF NOT EXISTS public.dim_US_City_Temperature (
	Date_Records     date NOT NULL,
	US_City          varchar(50),
	Country          varchar(30),
	Year             int4,
	Month_Name       varchar(25),
	Month_Num        int4,
    Day              int4,
    Avg_Temp         DOUBLE PRECISION,../float?,
    US_Port          varchar(15) NOT NULL,
    Port_State       varchar(25),
	CONSTRAINT pkey_US_Port PRIMARY KEY (US_Port)
    
);



#================= DEMOGRAPHY ========== STAGING & DIMENSION ===================================

#-------------------------- STAGING ----------------------------------------
CREATE TABLE IF NOT EXISTS public.Staging_US_City_Demog_Race (
    Demog_City                varchar(50) NOT NULL,
    State_Name                varchar(25),
    Male_Population           int4,
    Female_Population         int4,
    Total_Population          int4,
    Median_Age                DOUBLE PRECISION,
    Number_Of_Veterans        int4,
    Foreign_Born              int4,
    Average_Household_Size    DOUBLE PRECISION,
    American_Indian_and_Alaska_Native   int8,
    Asian_Population          int8,
    Black_or_African_American int8,
    Hispanic_or_Latino        int8,
    White_Population          int8,
    US_Port                   varchar(50),
    Port_State                varchar(50)
);

#------------------------DIMENSION-----------------------------------
CREATE TABLE IF NOT EXISTS public.dim_US_City_Demog_Race (
    Demog_City                varchar(50) NOT NULL,
    State_Name                varchar(25),
    Male_Population           int4,
    Female_Population         int4,
    Total_Population          int4,
    Median_Age                DOUBLE PRECISION,
    Number_Of_Veterans        int4,
    Foreign_Born              int4,
    Average_Household_Size    DOUBLE PRECISION,
    American_Indian_and_Alaska_Native   int8,
    Asian_Population          int8,
    Black_or_African_American int8,
    Hispanic_or_Latino        int8,
    White_Population          int8,
    US_Port                   varchar(50),
    Port_State                varchar(50),                                
    CONSTRAINT  pkey_US_Port PRIMARY KEY (US_Port)
   
);
    
#=========================== IMMIGRAITON DETAILS ================== STAGING & DIMENSION ===================
#--------------Fact_Immigration_Inclusive---------------------------

    CREATE TABLE IF NOT EXISTS  public.Staging_Fact_Immigration_Table (
	 Immigration_Id              int8 NOT NULL,
     Immigrants_Age              int4,
     Citizenship_Country         int4 NOT NULL,
     Residency_Country           int4,
     Current_State               varchar(20),
     Visa_Type                   varchar(25) NOT NULL,
     Immigration_Year            int4,
     Immigration_Month           int4,
     Port_Code                   varchar(25) NOT NULL,
     Arrival_Date                date NOT NULL,
     Departure_Date              date,
     Match_Flag                  varchar(5),
     Birth_Year                  int4,
     Gender                      varchar(10),
     Airline_Code                varchar(15),
     Admission_Num               int8 NOT NULL,
     Flight_Num                  varchar(15),
     Means_of_Travel             varchar(20),
     Visa_Purpose                varchar(25),
);

    
CREATE TABLE IF NOT EXISTS  public.Fact_Immigration_Table (
	 Immigration_Id              int8 NOT NULL,
     Immigrants_Age              int4,
     Citizenship_Country         int4 NOT NULL,
     Residency_Country           int4,
     Current_State               varchar(20),
     Visa_Type                   varchar(25) NOT NULL,
     Immigration_Year            int4,
     Immigration_Month           int4,
     Port_Code                   varchar(25) NOT NULL,
     Arrival_Date                date NOT NULL,
     Departure_Date              date,
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
    
#---------------Individual_Immigrants_Records ---------------- STAGING & DIMENSION -------------
    
CREATE TABLE IF NOT EXISTS public.Staging_Individual_Immigrants_Records(
     Entry_Num                     int8 NOT NULL,
     Immigration_Id                int8 NOT NULL,
     Arrival_Date                  date NOT NULL,
     Citizenship_Country           int4 NOT NULL,
     Immigrants_Age                int4,
     Gender                        varchar(5),
     Departure_Date                date,
     Visa_Type                     varchar(25) NOT NULL,
     Match_Flag                    varchar(5)
    

CREATE TABLE IF NOT EXISTS public.dim_Individual_Immigrants_Records(
     Entry_Num                     int8 NOT NULL,
     Immigration_Id                int8 NOT NULL,
     Arrival_Date                  date NOT NULL,
     Citizenship_Country           int4 NOT NULL,
     Immigrants_Age                int4,
     Gender                        varchar(5),
     Departure_Date                date,
     Visa_Type                     varchar(25) NOT NULL,
     Match_Flag                    varchar(5),
     CONSTRAINT pkey_Entry_Num  PRIMARY KEY(Entry_Num)
    
    
#-------------------------- Dated_Arrival_Departure----------------- STAGING & DIMENSION -----------

CREATE TABLE IF NOT EXISTS public.Staging_Dated_Arrival_Departure(
     Entry_Date                    date NOT NULL,
     Admission_Num                 int8 NOT NULL,
     Citizenship_Country           int4 NOT NULL,
     Arrival_Year                  int4,
     Arrival_Month                 varchar(15),
     Arrival_Day                   int4,
     Departure_Date                date,
     Depart_Year                   int4,
     Depart_Month                  varchar(15),
     Depart_Day                    int4,
     Visa_Type                     varchar(25) NOT NULL,
     Port_Code                     varchar(10)
     
           
CREATE TABLE IF NOT EXISTS public.dim_Dated_Arrival_Departure(
     Entry_Date                    date NOT NULL,
     Admission_Num                 int8 NOT NULL,
     Citizenship_Country           int4 NOT NULL,
     Arrival_Year                  int4,
     Arrival_Month                 varchar(15),
     Arrival_Day                   int4,
     Departure_Date                date,
     Depart_Year                   int4,
     Depart_Month                  varchar(15),
     Depart_Day                    int4,
     Visa_Type                     varchar(25) NOT NULL,
     Port_Code                     varchar(10),
     CONSTRAINT pkey_Entry_Date  PRIMARY KEY(Entry_Date)
    


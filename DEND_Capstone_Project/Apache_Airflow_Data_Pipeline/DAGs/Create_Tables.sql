
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
   #--> CONSTRAINT dim_Country_Codes_pkey PRIMARY KEY (Country_Codes)
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
    #---> CONSTRAINT dim_State_Codes_pkey PRIMARY KEY (State_Codes)
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

====================== CITY TEMPERATURE ========== STAGING & DIMENSION ==============================

#------Should I create a Yearly world city temperature staging table...?

----------------US CITY TEMPERATURE-----------------------------------
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
    #---> CONSTRAINT dim_US_City_Temperature_pkey PRIMARY KEY (US_Port)
);

#------Should I create a Yearly or Monthly Yearly city temperature staging table...?

================= DEMOGRAPHY ========== STAGING & DIMENSION ===================================

-------------------------- STAGING ----------------------------------------
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

------------------------DIMENSION-----------------------------------
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
    #---> CONSTRAINT dim_US_City_Demog_Race_pkey PRIMARY KEY (US_Port)
);
    
=========================== IMMIGRAITON DETAILS ================== STAGING & DIMENSION ===================
--------------Fact_Immigration_Inclusive---------------------------

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
    #---> CONSTRAINT Fact_Immigration_Table_pkey PRIMARY KEY(Immigration_Id)
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
    


    
    
=================================================================================================================
CREATE TABLE IF NOT EXISTS public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);

CREATE TABLE IF NOT EXISTS  public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);

CREATE TABLE IF NOT EXISTS  public.staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
);

CREATE TABLE IF NOT EXISTS  public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
);

CREATE TABLE IF NOT EXISTS  public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);

CREATE TABLE IF NOT EXISTS  public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);









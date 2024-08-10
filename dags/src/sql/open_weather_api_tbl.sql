drop table open_weather.open_weather_api_tbl
create table open_weather.open_weather_api_tbl (
id              INT,         
main            VARCHAR,        
description     VARCHAR,        
icon            VARCHAR,        
base            VARCHAR,        
visibility      INT,         
dt              INT,         
timezone        INT,         
name            VARCHAR,        
cod             INT,         
coord_lon       DOUBLE PRECISION,       
coord_lat       DOUBLE PRECISION,       
main_temp       DOUBLE PRECISION,       
main_feels_like DOUBLE PRECISION,       
main_temp_min   DOUBLE PRECISION,       
main_temp_max   DOUBLE PRECISION,       
main_pressure   INT,         
main_humidity   INT,         
main_sea_level  INT,        
main_grnd_level INT,        
wind_speed      DOUBLE PRECISION,       
wind_deg        INT,         
wind_gust       DOUBLE PRECISION,        
clouds_all      INT,         
sys_country     VARCHAR,        
sys_sunrise     INT,         
sys_sunset      INT,         
sys_type        DOUBLE PRECISION,        
sys_id          DOUBLE PRECISION,        
date_time       TIMESTAMP
);



select * from open_weather.open_weather_api_tbl


delete from open_weather.open_weather_api_tbl

select * from public.open_weather_api_tbl
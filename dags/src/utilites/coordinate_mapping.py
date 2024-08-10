from utilites import json_reader
import requests
import os
import json
from sqlalchemy import create_engine
import pandas as pd

print(json_reader.open_weather_api_key)
print(json_reader.open_weather_base_url)




def df_map_base_url_with_coordinates():
    f = open(json_reader.coordinates_files_path)
    # returns JSON object as
    # a dictionary
    response_list = []
    data = json.load(f)
    # Iterating through the json
    # list
    for i in data:
        # print(i['Lat'],i['Long'])
        base_url = (json_reader.open_weather_base_url + '?' + 'lat=' + str(i['Lat']) + '&' + 'lon=' + str(
            i['Long']) + '&' + 'appid=' + json_reader.open_weather_api_key + '&' + 'units=metric')

        response = requests.get(base_url)
        response_list.append(response.json())
    #return(response_list)
    #data frame write
    df = pd.json_normalize(response_list, "weather",
                           ['base', 'visibility', 'dt', 'timezone', 'name', 'cod', ["coord", "lon"], ["coord", "lat"],
                            ['main', 'temp'], ['main', 'feels_like'], ['main', 'temp_min'], ['main', 'temp_max'],
                            ['main', 'pressure'], ['main', 'humidity'], ['main', 'sea_level'], ['main', 'grnd_level'],
                            ['wind', 'speed'], ['wind', 'deg'], ['wind', 'gust'],
                            ['clouds', 'all'],
                            ['sys', 'country'], ['sys', 'sunrise'], ['sys', 'sunset'], ['sys', 'type'], ['sys', 'id'],
                            ], errors='ignore')

    return df

#map_base_url_with_coordinates()


### dataframe cleansing

def data_cleansing(dataframe):
    def replace_nan(x):
        if pd.isna(x):
            if pd.api.types.is_object_dtype(type(x)):
                return "null"
            else:
                return 0
        return x

    # Apply the replacement function
    dataframe = dataframe.applymap(replace_nan)

    # city name decoding
    from unicodedata import normalize
    dataframe['name'] = dataframe.name.apply(lambda x: normalize('NFD', x).encode(
        'ascii', 'ignore').decode('utf-8-sig'))

    # from datetime import datetime
    dataframe['date_time'] = pd.to_datetime(dataframe['dt'], unit='s')

    ## rename column names
    dataframe.rename(columns=lambda x: x.replace('.', '_'), inplace=True)

    return dataframe


# write to postgres DB
def write_to_postgres_db(dataframe):
    with open(json_reader.postgres_files_path, 'r') as f:
        db_params = json.load(f)


    # Create the connection string
    connection_string = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
            #connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

    print(connection_string)
    # Create the database engine
    engine = create_engine(connection_string)

    # Insert the DataFrame into the PostgreSQL Database
    # Define the table name and schema
    table_name = 'open_weather_api_tbl'
    schema_name = 'open_weather'

    # Insert the DataFrame into the PostgreSQL table
    dataframe.to_sql(table_name,engine,schema_name, if_exists='append', index=False)

    print("Data inserted successfully")

# write to local
def write_to_local(dataframe):
    dataframe.to_csv(json_reader.write_local_files_path)








import configparser
import json
import os
config = configparser.RawConfigParser()


config_dir = os.path.dirname(os.path.abspath(__file__))
print(config_dir)
config_dir_path=os.path.normpath(config_dir + os.sep + os.pardir)

# Construct the path to the config file relative to the script's directory
config_path = os.path.join(config_dir_path, 'config', 'api_properties.properties')

print(config.read(config_path))


coordinates_files_path=os.path.join(config_dir_path,'config','cities_coordinates.json')

open_weather_api_key=config.get('open_weather_api_properties', 'open_weather_api_key')
open_weather_base_url=config.get('open_weather_api_properties', 'open_weather_base_url')
print(open_weather_base_url,open_weather_api_key)

print(coordinates_files_path)


## postgres db propperties

postgres_files_path=os.path.join(config_dir_path,'config','postgres_db_properties.json')
print(postgres_files_path)

# write to local
write_local_files_path=os.path.join(config_dir_path,'output','finalOpenWeatherData.csv')
print(write_local_files_path)



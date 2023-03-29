######################################################################################################################################################
# Code Info                                                                                                                                          #
#                                                                                                                                                    #
# data_parser.py                                                                                                                                     #
# Author(s): Varun Pius Rodrigues                                                                                                                    #
# About: Get data from API and store in local directory                                                                                              #
######################################################################################################################################################


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Library Imports goes here
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

# System Libraries
import datetime
import time
import json
import logging

# Internal modules
from src import basedir, datadir, confdir

# External librabries
import requests
from ruamel.yaml import YAML


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Configurations goes here
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

# Logging configurations
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO, encoding='UTF-8', format='%(levelname)s: %(message)s')
logging.info('Main start')


######################################################################################################################################################
# Code starts here
######################################################################################################################################################

# Reading configurations
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
def config_reader():
    logging.info('Reading API keys and coordinates of cities')
    config_json = {}
    yaml=YAML(typ='safe')
    config_file = confdir + '/config.yml'
    with open(config_file, 'r') as file:
        config = yaml.load(file)

    API_key = config['API_key']
    logging.info('API Key read')

    cities_dict = {}
    for city in config['Cities']:
        addr = []
        addr.append(config['Cities'][city]['City'])
        addr.append(config['Cities'][city]['Country'])
        if 'State' in config['Cities'][city]:
            addr.append(config['Cities'][city]['State'])
        cities_dict[city] = addr

    logging.info('Coordinates of cities read')

    return API_key, cities_dict


# Get coordinates of the city; APIs use coordinates rather than city names
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
def get_coordinates(cities_dict):
    logging.info('Getting coordinates for cities')
    city_coor = {}

    for location in cities_dict:
        city = cities_dict[location][0]
        country = cities_dict[location][1]
        logging.info('Processing for ' + city)

        if len(cities_dict[location]) == 3:
            state = cities_dict[location][2]
            url = f"http://api.openweathermap.org/geo/1.0/direct?q={city},{state},{country}&appid=3062d4ce0a34f2ef6c2f35be5dd1ce9c"
        else:
            url = f"http://api.openweathermap.org/geo/1.0/direct?q={city},{country}&appid=3062d4ce0a34f2ef6c2f35be5dd1ce9c"
    
        loc_resp = requests.get(url)
        response_json = loc_resp.json()

        coor = []
        city_name = city + ", " + country
        coor.append(city_name)
        coor.append(response_json[0]['lat'])
        coor.append(response_json[0]['lon'])

        city_coor[city] = coor

    logging.info('Coordinates for cities processed')
    return city_coor


# Convert Epoch timestamp in dataset to readable date format
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
def process_epoch_time(epoch_time):
    logging.info("Converting epoch time to readable format")
    date_time = datetime.datetime.fromtimestamp( epoch_time )
    dt = date_time.strftime("%Y-%m-%d")

    return dt


# Convert Epoch timestamp in dataset to readable date format
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
def get_epoch_time():
    logging.info("Converting UTC to epoch time")

    today = datetime.date.today()
    date_time = datetime.datetime(today.year, today.month, today.day, 11, 30)
    epoch_time_today = time.mktime(date_time.timetuple())

    epoch_times = []
    epoch_times.append(int(epoch_time_today))
    for i in range(1, 5):
        ep_calc = epoch_time_today - (i * 86400)
        epoch_times.append(int(ep_calc))

    return epoch_times

     
# Get data from APIs
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
def get_data(API_key, city, ep_tm):
    logging.info("Initializing city parameters")
    lat = city[1]
    lon = city[2]

    url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={ep_tm}&appid={API_key}"

    logging.info("Sending weather data request")
    weather_resp = requests.get(url)

    logging.info("Weather data response received")
    weather_json = weather_resp.json()
    logging.info(weather_json)

    # Replacing Epoch with Date
    for dly_data in weather_json['data']:
        dly_data['dt'] = process_epoch_time(dly_data['dt'])
    
    return weather_json['data'][0]


# Processing data and writing to output
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
def process_data(API_key, city_coor):
    logging.info("Processing data")
    city_weather_data = [] # {}

    epoch_times = get_epoch_time()

    for city_name, city_vals in city_coor.items():
        #city_weather_data[city_name] = []
        for ep_tm in epoch_times:
            logging.info("Getting weather data for " + city_name)
            dly_data = get_data(API_key, city_vals, ep_tm)
            dly_data['city'] = city_vals[0]
            city_weather_data.append(dly_data) #city_weather_data[city_name].append(dly_data)

    logging.info("Writing daily weather data to JSON file")
    city_weather_data_file = datadir + '/staging/city_weather_data.json'
    with open(city_weather_data_file, "w") as outfile:
        json.dump(city_weather_data, outfile)
    
    return


# Main starter
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
def start():
    logging.info("Data Parser started")
    API_key, cities_dict = config_reader()
 
    city_coor = get_coordinates(cities_dict)
    city_names_file = datadir + '/staging/city_names.json'
    with open(city_names_file, "w") as outfile:
        json.dump(city_coor, outfile)

    process_data(API_key, city_coor)


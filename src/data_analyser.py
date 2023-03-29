######################################################################################################################################################
# Code Info                                                                                                                                          #
#                                                                                                                                                    #
# data_parser.py                                                                                                                                     #
# Author(s): Varun Pius Rodrigues                                                                                                                    #
# About: Process data stored in local directory and return results                                                                                   #
######################################################################################################################################################


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Library Imports goes here
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

# System Libraries
import os
import datetime
import logging

# Internal modules
from src import basedir, datadir, confdir

# External librabries
import pandas as pd


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

'''
Method to analyse dataset and return the location, date and temperature of the highest temperatures reported by location and month.
Basically, I grouped by `location` and found date with the highest temperature, and display them together.
The output will be location, the highest temperature for the city as well as date when the temperature was reported
'''
def get_city_with_max_temp(df):
    logging.info('Aggregating on location and month level for highest temp')
    df1 = df.copy()
    #df1['Date'] = df1.dt.to_datetime
    df1['DtConverted'] = pd.to_datetime(df1['dt'])
    df1['month'] = df1['DtConverted'].dt.month
    logging.info('Month extracted from date')

    df1['max_temp'] = df1.groupby(['city', 'month'])['temp'].transform(lambda x: x.rank(ascending = False))
    df1 = df1[['city', 'dt', 'temp']].loc[df1.max_temp == 1]
    logging.info('Final dataframe_1 with filtered columns done')

    # Results
    #result_df1 = df1.to_string(index=False)
    result_df1 = df1.to_json(orient = 'records')        # Converting Dataframe to JSON format for HTTP response
    print(result_df1)

    return result_df1


'''
Method to analyse dataset and return JSON containing the average temperature, Minimum temperature, 
    location of minimum temperature, and location of maximum temperature per day.
The output will be date, avergage temperature, Minimum temperature, as well as location of minimum and maximum temperatures for that date
'''
def get_weather_agg(df):
    logging.info('Aggregating on daily level')
    df2 = df.copy()
    
    logging.info('Aggregation 2 starts here ')
    df2['avg_temp'] = df2.groupby('dt')['temp'].transform(lambda x: x.mean())
    df2['min_temp'] = df2.groupby('dt')['temp'].transform(lambda x: x.min())
    df2['max_temp'] = df2.groupby('dt')['temp'].transform(lambda x: x.max())

    logging.info('Finding cities with max and min temperature')
    df2['min_temp_loc'] = df2.city.loc[df2.min_temp == df2.temp] 
    df2['max_temp_loc'] = df2.city.loc[df2.max_temp == df2.temp]

    # Selecting only necessary columns
    logging.info('Filtering necessary columns')
    df2 = df2[['dt', 'avg_temp', 'min_temp', 'max_temp', 'min_temp_loc', 'max_temp_loc']]
    print(df2)

    # Deleting duplicate columns with Nan values
    logging.info('Deleting columns with Nan values (they dont have either the highest or lowest values)')
    df2.dropna(subset=['min_temp_loc', 'max_temp_loc'], how='all', inplace=True)
    print(df2)

    # Dataframe for reporting cities with max temp and min temp
    logging.info('Merging the rows with max and min temp city values')
    df_temp_city = df2.groupby(['dt']).agg(
                    {'min_temp_loc': lambda x: x.dropna().iloc[0], 
                    'max_temp_loc': lambda x: x.dropna().iloc[0]})
    print(df_temp_city)
    
    # Numeric columns
    logging.info('deleying duplicates for aggregation columns')
    df_agg = df2[['dt', 'avg_temp', 'min_temp']]
    df_agg.drop_duplicates(inplace=True)
    print(df_agg.sort_values('dt'))

    final_df = df_agg.merge(df_temp_city, on='dt')

    logging.info('Final values sorted by date')
    print(final_df.sort_values('dt'))
    
    # Results
    #result_df2 = final_df.to_string(index=False)
    result_df2 = final_df.to_json(orient = 'records')   # Converting Dataframe to JSON format for HTTP response
    print(result_df2)

    return result_df2


'''
Alternative approach for above method
'''
def get_weather_agg_alt(df):
    df2 = df.copy()
    
    df2['avg_temp'] = df2.groupby('dt')['temp'].transform(lambda x: x.mean())
    df2['min_temp'] = df2.groupby('dt')['temp'].transform(lambda x: x.min())
    df2['max_temp'] = df2.groupby('dt')['temp'].transform(lambda x: x.max())

    df_min_city = df2[['dt', 'city']].loc[df2.min_temp == df2.temp].rename(columns={'city': 'min_city'})
    print(df_min_city)

    df_max_city = df2[['city', 'dt']].loc[df2.max_temp == df2.temp].rename(columns={'city': 'max_city'})
    print(df_max_city)

    df_temp_city = df_min_city.merge(df_max_city, on='dt')
    print("Min Max Temp City:")
    print(df_temp_city)

    df2 = df2[['dt', 'avg_temp', 'min_temp', 'max_temp']]
    df2.drop_duplicates(inplace=True)
    print(df2.sort_values('dt'))

    final_df = df2.merge(df_temp_city, on='dt')
    print("Final DF:")
    print(final_df)
    return final_df


def process_dataframe():
    logging.info('Runner for fetching results')
    stage_data_file = datadir + "/staging/city_weather_data.json"
    df = pd.read_json(stage_data_file)

    result_df1 = get_city_with_max_temp(df)
    result_df2 = get_weather_agg(df)

    logging.info('Results parsed and returning as JSON')
    return result_df1, result_df2




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
import os

# Internal modules
from src import basedir, datadir, confdir

# External librabries
import pandas as pd


def get_city_with_max_temp(df):
    df1 = df.copy()
    df1['max_temp'] = df1.groupby('city')['temp'].transform(lambda x: x.rank(ascending = False))
    df1 = df1[['city', 'dt', 'temp']].loc[df1.max_temp == 1] 

    # Results
    #result_df1 = df1.to_string(index=False)
    result_df1 = df1.to_json(orient = 'records')
    print(result_df1)

    return result_df1


def get_weather_agg(df):
    df2 = df.copy()
    
    df2['avg_temp'] = df2.groupby('dt')['temp'].transform(lambda x: x.mean())
    #df2['avg_temp'] = df2.groupby('dt')['temp'].agg('mean')
    df2['min_temp'] = df2.groupby('dt')['temp'].transform(lambda x: x.min())
    df2['max_temp'] = df2.groupby('dt')['temp'].transform(lambda x: x.max())
    df2['min_temp_loc'] = df2.city.loc[df2.min_temp == df2.temp] 
    df5 = df2.city.loc[df2.min_temp == df2.temp] 
    print(df5)

    df2['max_temp_loc'] = df2.city.loc[df2.max_temp == df2.temp]

    # Selecting only necessary columns
    df2 = df2[['dt', 'avg_temp', 'min_temp', 'max_temp', 'min_temp_loc', 'max_temp_loc']]
    print("Debug 1:")
    print(df2)

    # Deleting duplicate columns with Nan values
    df2.dropna(subset=['min_temp_loc', 'max_temp_loc'], how='all', inplace=True)
    print("Debug 2:")
    print(df2)


    # Dataframe for reporting cities with max temp and min temp
    df_temp_city = df2.groupby(['dt']).agg(
                    {'min_temp_loc': lambda x: x.dropna().iloc[0], 
                    'max_temp_loc': lambda x: x.dropna().iloc[0]})
    print("Debug 3:")
    print(df_temp_city)
    
    # Numeric columns
    df_agg = df2[['dt', 'avg_temp', 'min_temp', 'max_temp']]
    df_agg.drop_duplicates(inplace=True)
    print("Debug 4:")
    print(df_agg.sort_values('dt'))

    final_df = df_agg.merge(df_temp_city, on='dt')

    print("Debug 5:")
    print(final_df.sort_values('dt'))
    
    # Results
    #result_df2 = final_df.to_string(index=False)
    result_df2 = final_df.to_json(orient = 'records')
    print(result_df2)

    return result_df2


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
    stage_data_file = datadir + "/staging/city_weather_data.json"
    df = pd.read_json(stage_data_file)

    result_df1 = get_city_with_max_temp(df)
    result_df2 = get_weather_agg(df)

    return result_df1, result_df2




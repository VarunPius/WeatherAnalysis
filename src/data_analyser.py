import pandas as pd

df = pd.read_json('../data/staging/city_weather_data.json')


def part1():
    df1 = df.copy()
    df1['max_temp'] = df1.groupby('city')['temp'].transform(lambda x: x.rank(ascending = False))
    df1 = df1[['city', 'dt', 'temp']].loc[df1.max_temp == 1] 

    print(df1)

    print(df1.index)
    #print(df1)
    df_p1 = df1.to_string(index=False)
    print(df_p1)


def part2():
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
    return final_df


def part3():
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


if __name__ == '__main__':
    part1()
    df2 = part2()
    df3 = part3()
    print("Final")
    print("Final")
    print(df2)
    print(df3)


'''
- Build another repository of data that will contain the results of the following calculations from the data stored in step 2.
    - A dataset containing the location, date and temperature of the highest temperatures reported by location and month.
    - A dataset containing the average temperature, min temperature, location of min temperature, and location of max temperature per day.
'''


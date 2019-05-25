import os
import requests
import json
import csv
import sqlite3
import urllib3
import sys
import pandas as pd
from pprint import pprint
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import seaborn as sns
from sqlalchemy import create_engine
from pandas.io.json import json_normalize
import pandas.io.sql as psql
import psycopg2
import mysql.connector
import appscript 
import subprocess
import pickle
from datetime import datetime, timezone, timedelta
import datetime as dt
import time
import re

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)
# pd.set_option('display.width', 1000)
# plt.style.use('seaborn-white')


# appscript.app('Terminal').do_script('/usr/local/mysql/bin/mysql -u root -p')


urllib3.disable_warnings()
pd.options.display.max_seq_items

#Receiving API Data From Darksky in json format
#TODO: Delete this
# http = urllib3.PoolManager()
# r = http.request('GET',"https://api.darksky.net/forecast/ac89150eb898f7dda846b45ca4896211/37.8267,-122.4233",retries=3)
# r.status
# darkSkyData = json.loads(r.data.decode('utf-8'))

#Source File for top 50 most populated cities in the United States



#TODO: timestamp API pull

def getWeatherData():
    darkSkyList = []
    cityDF = pd.read_csv('./foocity.csv')
    cityData = cityDF.to_dict('series')
    for (k,v), (k2,v2),(k3, v3), (k4, v4), (k5,v5),(k6,v6) in zip(cityData['lat'].items(),cityData['lng'].items(),\
        cityData['city'].items(),cityData['state_id'].items(),cityData['county_name'].items(),cityData['id'].items()):
        http = urllib3.PoolManager()
        r = http.request('GET',"https://api.darksky.net/forecast/ac89150eb898f7dda846b45ca4896211/{},{}?exclude=minutely,hourly,daily,alerts".format(v, v2),\
                 retries=3)
        data = json.loads(r.data.decode('utf-8'))
        data['city'] = v3
        data['state_id'] = v4
        data['county_name'] = v5
        data['city_id'] = v6
        darkSkyList.append(data)
        df = json_normalize(darkSkyList)
        #DarkSky_JSON_API is utilized for observing the JSON API pulls before further manipulation
        df.to_csv('/Users/ericrivetna/desktop/data analysis/DarkSky_JSON_API.csv') 
        dataCSV = pd.read_csv('/Users/ericrivetna/desktop/data analysis/DarkSky_JSON_API.csv')
        #Converted to a DataFrame to rename columns using Pandas Library
        darkSkyDF = pd.DataFrame(dataCSV)

        darkSkyDF.rename(columns = lambda x: x.replace('currently.','curr_')[0:],inplace = True)
        darkSkyDF.rename(columns = lambda x: x.replace('current_time','curr_time')[0:],inplace = True)
        darkSkyDF.rename(columns = lambda x: x.replace('Unnamed: 0','request_id')[0:],inplace = True)
        darkSkyDF.rename(columns = lambda x: x.replace('curr_summary','curr_conditions')[0:],inplace = True)
        darkSkyDF.rename(columns = lambda x: x.replace('curr_icon','curr_expanded_summary')[0:],inplace = True)
        darkSkyDF['curr_time'] = pd.to_datetime(darkSkyDF['curr_time'], unit='s')
        darkSkyDF = darkSkyDF[['request_id','city_id','city','state_id', \
                                'county_name','latitude','longitude', \
                                'curr_time','curr_temperature','curr_apparentTemperature',\
                                'curr_conditions','curr_expanded_summary','curr_precipIntensity',\
                                'curr_dewPoint','curr_humidity','curr_precipProbability',\
                                'curr_cloudCover','curr_windSpeed',\
                                'curr_windGust','curr_windBearing','curr_visibility',\
                                'curr_nearestStormDistance'
                                ]]
        # DarkSkyDB is the Database file that will be inserted into SQL/used as Pandas DataFrame 
        darkSkyDF.to_csv('/Users/ericrivetna/desktop/data analysis/DarkSkyDB1.csv')

        
    return darkSkyDF


# darkSkyDF = pd.DataFrame(getWeatherData())
# print(darkSkyDF.head)
# print(darkSkyDF.shape)

#TODO: Figure out how to rename index & request_id
def appenddata():
        with open('/Users/ericrivetna/desktop/data analysis/DarkSkyDB1.csv') as inp, \
             open('/Users/ericrivetna/desktop/data analysis/DarkSkyDB.csv','a') as out:
             reader = csv.DictReader(inp)
             writer = csv.DictWriter(out,fieldnames=['','request_id','city','city_id','state_id',
                                                     'county_name','latitude','longitude','curr_time',
                                                     'curr_temperature','curr_apparentTemperature',
                                                     'curr_conditions','curr_expanded_summary','curr_precipIntensity',
                                                     'curr_dewPoint','curr_humidity','curr_precipProbability','curr_cloudCover',
                                                     'curr_windSpeed','curr_windGust','curr_windBearing',
                                                     'curr_visibility','curr_nearestStormDistance'])
             for row in reader:
                     if out.tell() == 0:
                             writer.writeheader()
                     writer.writerow(row)
                     
                  
appenddata()

# def sql_connect():
#         con = mysql.connector.connect(user="root",
#         password="Cheesecloth1", host="localhost",
#         port=3306)
#         con.autocommit = True
#         cur = con.cursor()
#         cur.execute("""Use DarkSky""")
        

#Restructure Tables to actual Relational Database
def mySQL_table_creation():
        """UNIQUE GARBAGE PASSWORD USED FOR mySQL"""
        #TODO:  Google how to hide private information in source code
        con = mysql.connector.connect(user="root",
                password="Cheesecloth1", host="localhost",
                port=3306)
        con.autocommit = True
        cur = con.cursor()
        
        cur.execute('CREATE DATABASE IF NOT EXISTS DarkSky')
       
#TODO: I know this Table Schema is not normalized. Think of a solution for applying normalization.              
        
        cur.execute("""Use DarkSky""")
        create_geo_table = """CREATE TABLE IF NOT EXISTS darksky_geo
                (city_id VARCHAR(255),
                city VARCHAR(255) NOT NULL,
                state_id VARCHAR(255) NOT NULL,
                county_name TEXT NOT NULL,
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                curr_time DATETIME NOT NULL,
                CONSTRAINT pk_DateLocationPairIDs PRIMARY KEY (city_id,curr_time,latitude,longitude));"""
        cur.execute(create_geo_table)
         
        create_weather_table = """CREATE TABLE IF NOT EXISTS darksky_weather
                (city_id VARCHAR(255) PRIMARY KEY, 
                city VARCHAR(255) NOT NULL,
                state_id VARCHAR(255) NOT NULL,
                curr_time DATETIME NOT NULL,
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                curr_temperature DECIMAL NOT NULL,
                curr_apparentTemperature FLOAT NOT NULL,
                curr_conditions TEXT NOT NULL,
                curr_expanded_summary VARCHAR(255),
                curr_precipIntensity INT,
                curr_dewPoint FLOAT,
                curr_humidity FLOAT,
                curr_cloudcover FLOAT,
                curr_precipProbability FLOAT,
                curr_windSpeed FLOAT,
                curr_windGust FLOAT,
                curr_windBearing INT,
                curr_visibility FLOAT, 
                curr_nearestStormDistance INT,
                FOREIGN KEY fk_DateLocationPairIDs(city_id,curr_time) REFERENCES darksky_geo(city_id,curr_time));"""
        
        cur.execute(create_weather_table)

        create_austin_311_table = """CREATE TABLE IF NOT EXISTS austin_311
                (id INT AUTO_INCREMENT PRIMARY KEY,
                 sr_request_id VARCHAR(255),
                 sr_type_desc TEXT,
                 sr_method_received_desc TEXT,
                 sr_status_desc TEXT,
                 sr_status_date DATETIME,
                 sr_created_date DATETIME,
                 sr_updated_date DATETIME,
                 sr_closed_date DATETIME,
                 sr_location VARCHAR(255),
                 sr_location_street_number INT,
                 sr_location_street_name TEXT,
                 sr_location_city TEXT,
                 sr_location_zip_code INT,
                 sr_location_county TEXT,
                 sr_location_x FLOAT,
                 sr_location_y FLOAT,
                 sr_location_long FLOAT,
                 sr_location_lat FLOAT,
                 sr_location_lat_long VARCHAR(255),
                 sr_location_council_district INT,
                 sr_location_map_page VARCHAR(255),
                 sr_location_map_tile VARCHAR(255));"""
        
        cur.execute(create_austin_311_table)



mySQL_table_creation()




#Load Data Into mySQL database
def mySQL_data_insert():
        """UNIQUE GARBAGE PASSWORD USED FOR mySQL"""
        #TODO:  Google how to hide private information in source code
        #TODO: Make the SQL connection a function
        con = mysql.connector.connect(user="root",
        password="Cheesecloth1", host="localhost",
        port=3306)
        con.autocommit = True
        cur = con.cursor()
        cur.execute("Use DarkSky")

        """Inserting data from CSV to SQL without Pandas"""
        #TODO: Read more about how to do this with strictly Python/CSV Module when working with larger datasets as to not always rely on Pandas Library
        
        with open('/Users/ericrivetna/desktop/data analysis/DarkSkyDB.csv', 'r') as f:
            reader = csv.DictReader(f)
            to_db_geo = [(i['city_id'],i['city'],i['state_id'],i['county_name'],i['latitude'],i['longitude'],i['curr_time']) for i in reader]
            cur.execute("delete from darksky_geo;")
            cur.executemany("INSERT INTO darksky_geo (city_id,city,state_id,county_name,latitude,longitude,\
                             curr_time) VALUES (%s,%s,%s,%s,%s,%s,%s);",to_db_geo)
        
        """Using Pandas/SQLalchemy to create SQL database from CSV file DarkSkyDB"""
        
        engine = create_engine('mysql+mysqlconnector://root:Cheesecloth1@localhost/darksky')

        darksky_weather_df = pd.read_csv('/Users/ericrivetna/desktop/data analysis/DarkSkyDB.csv')
        darksky_weather_df = pd.DataFrame(darksky_weather_df)
        darksky_weather_df.drop(['request_id','county_name','Unnamed: 0'],axis=1,inplace=True)
        darksky_weather_df['curr_time'] = pd.to_datetime(darksky_weather_df['curr_time'])
        
        
        # darksky_geo_requests = pd.read_sql_table('darksky_geo',con=engine)
        # darksky_weather_requests = pd.read_sql_table('darksky_geo',con=engine)


        """Using Pandas to Insert Darksky Data into mySQL Database"""
        darksky_weather_df.to_sql(name='darksky_weather', con=engine, if_exists='replace',index=False)

        #TODO: Write an if statement that checks if this block code needs to be executed. The load time to read_csv is too long. 
        # austin_311_df = pd.read_csv('/Users/ericrivetna/desktop/data analysis/Austin_311_two.csv', dtype='unicode')
        # austin_311_df = pd.DataFrame(austin_311_df)
        # austin_311_df['sr_status_date'] = pd.to_datetime(austin_311_df['sr_status_date'])
        # austin_311_df['sr_created_date'] = pd.to_datetime(austin_311_df['sr_created_date'])
        # austin_311_df['sr_updated_date'] = pd.to_datetime(austin_311_df['sr_updated_date'])
        # austin_311_df['sr_closed_date'] = pd.to_datetime(austin_311_df['sr_closed_date'])
        # austin_311_df.dropna(axis=0,how='any',inplace=True,thresh=3)

        # try:
        #         austin_311_df.to_sql(name='austin_311', con=engine, if_exists='fail',index=False,chunksize=1000)
        # except ValueError:
        #         print('austin_311 table already exists')

# mySQL_data_insert()

def austin_311_pickle():
        """Querying the SQL Database takes upwards of 2 minutes to run. Pickling the DataFrame object decreases retrieval time to <1 Seconds"""
        con = mysql.connector.connect(user="root",
        password="Cheesecloth1", host="localhost",
        port=3306)
        con.autocommit = True
        cur = con.cursor()
        cur.execute("Use DarkSky")
        engine = create_engine('mysql+mysqlconnector://root:Cheesecloth1@localhost/darksky')
        
        austin_311_df = pd.read_sql_table('austin_311',con=engine)
        austin_311_df = pd.DataFrame(austin_311_df)
        austin_311_df = austin_311_df.to_pickle("austin_311_df.pkl")


# austin_311_pickle()


def getWeatherData_311():
    """Opening austin_311_df DataFrame object with Pickle"""
    austin_darksky_311 = []
    austin_311_df = pd.read_pickle("austin_311_df.pkl")
    austin_311_df.dropna(inplace=True)  

    """Standarized Lat/Long by truncating values to reduce number of API calls from 600k+ to ~3k"""

    austin_311_df['sr_location_lat_trunc'] = austin_311_df['sr_location_lat'].astype('str').str[0:5]
    austin_311_df['sr_location_long_trunc'] = austin_311_df['sr_location_long'].astype('str').str[0:6]
    


    """Converting Dates back into Epoch for API call"""
    rng = pd.date_range('2013-12-31 00:00:00','2019-03-27 17:24:00',freq='min')

    austin_311_df['sr_status_date'] = austin_311_df['sr_status_date'].dt.round("H")
    austin_311_df['sr_status_date'] = (austin_311_df['sr_status_date'] - dt.datetime(1970,1,1))
    austin_311_df['sr_status_date'] = austin_311_df['sr_status_date'].values.astype(np.int64) // 10**6
    austin_311_api_df = pd.concat([austin_311_df['sr_status_date'],austin_311_df['sr_location_lat_trunc'],austin_311_df['sr_location_long_trunc']],axis=1)   
  
    austin_311_api_df = austin_311_api_df.sort_values('sr_status_date',ascending=False)
    austin_311_api_df = austin_311_api_df.drop_duplicates(['sr_location_lat_trunc','sr_location_long_trunc','sr_status_date'])[['sr_location_lat_trunc','sr_location_long_trunc','sr_status_date']]
    
    austin311DarkSkyList = []
    """ONLY TO BE RAN ONCE - Large API Call to DarkSky"""
#     for index, row in austin_311_api_df.iterrows():    
#             http = urllib3.PoolManager()
#             r = http.request('GET',"https://api.darksky.net/forecast/ac89150eb898f7dda846b45ca4896211/{},{},{}?exclude=minutely,hourly,daily,alerts,flags".format(row['sr_location_lat_trunc'],row['sr_location_long_trunc'],str(row['sr_status_date'])[0:10]),retries=3)
#             data = json.loads(r.data.decode('utf-8'))
#             austin311DarkSkyList.append(data)
#             df = json_normalize(austin311DarkSkyList)
#             df.to_csv('/Users/ericrivetna/desktop/data analysis/AustinDarkSky311_JSON_API.csv')
 

getWeatherData_311()

def histogram_austin_311():
        austin_311_df = pd.read_pickle("austin_311_df.pkl")
        """Visualizing the Top 10 Call Types to Austin 311"""
        """SQL Command SELECT sr_type_desc, COUNT(*) as 'num_of_occurences' FROM austin_311 GROUP BY sr_type_desc ORDER BY COUNT(*) DESC LIMIT 10;"""
        histogram_count_dict = austin_311_df['sr_type_desc'].value_counts().nlargest(10).to_dict()
        plt.bar(histogram_count_dict.keys(),histogram_count_dict.values(),width=0.8)
        plt.xlabel('Types of Calls', fontsize=9)
        plt.ylabel('No. of occurences', fontsize=12)
        plt.xticks(fontsize=8.7, rotation=45)
        plt.title('Austin 311 Calls by Type')
        # plt.show()

        """Visualizing the Top 10 Zip Codes calling Austin 311"""
        """SQL Command select sr_location_zip_code, COUNT(*) sr_location_zip_code FROM austin_311 GROUP BY sr_location_zip_code ORDER BY COUNT(*) DESC LIMIT 10;"""
        zip_count_dict = austin_311_df['sr_location_zip_code'].value_counts().nlargest(10).to_dict()
        plt.bar(zip_count_dict.keys(),zip_count_dict.values(),width=0.8)
        plt.xlabel('Austin Zipcodes')
        plt.ylabel('Volume of calls per Zipcode')
        plt.xticks(fontsize=8.7, rotation=45)
        plt.title('Volume of Austin 311 Calls per Zipcode')
        # plt.show()

        """Obtaining Year of Status Date from austin_311_df. Required cleaning last 2 characters"""
        austin_311_df['Year'] = austin_311_df['sr_status_date'].dt.year
        austin_311_df['Year'] = austin_311_df['Year'].map(lambda x: str(x)[:-2])
        """Obtaining Month of Status Date from austin_311_df. Required cleaning last 2 characters"""
        austin_311_df['Month'] = austin_311_df['sr_status_date'].dt.month
        austin_311_df['Month'] = austin_311_df['Month'].map(lambda x: str(x)[:-2])
        """Obtaining Day of Status Date from austin_311_df. Required cleaning last 2 characters"""
        austin_311_df['Day'] = austin_311_df['sr_status_date'].dt.day
        austin_311_df['Day']= austin_311_df['Day'].map(lambda x: str(x)[:-2])

        df1 = austin_311_df[['Year','sr_location_zip_code']]
        df1.dropna(axis='columns',inplace=True)
        # zipcode_by_year = df1.pivot_table(index=austin_311_df['Year'],columns=['sr_location_zip_code'],aggfunc='count', fill_value=0)
        """Quick Data Cleaning with CSV file"""
        df1.to_csv('zipcode_by_year2.csv')
        zipcode_by_year = pd.read_csv('zipcode_by_year.csv')
        
        


# histogram_austin_311()

# def dates():
#         austin_311_df = pd.read_pickle("austin_311_df.pkl")
#         """Obtaining Year of Status Date from austin_311_df. Required cleaning last 2 characters"""
#         austin_311_df['Year'] = austin_311_df['sr_status_date'].dt.year
#         austin_311_df['Year'] = austin_311_df['Year'].map(lambda x: str(x)[:-2])
#         """Obtaining Month of Status Date from austin_311_df. Required cleaning last 2 characters"""
#         austin_311_df['Month'] = austin_311_df['sr_status_date'].dt.month
#         austin_311_df['Month'] = austin_311_df['Month'].map(lambda x: str(x)[:-2])
#         """Obtaining Day of Status Date from austin_311_df. Required cleaning last 2 characters"""
#         austin_311_df['Day'] = austin_311_df['sr_status_date'].dt.day
#         austin_311_df['Day']= austin_311_df['Day'].map(lambda x: str(x)[:-2])
#         print(austin_311_df['sr_location_zip_code'].value_counts().sum())

#         df1 = austin_311_df[['Year','sr_location_zip_code']]
#         zipcode_by_year = df1.pivot_table(index=austin_311_df['Year'],columns=['sr_location_zip_code'],aggfunc='count', fill_value=0)
#         zipcode_by_year.drop(['n'],inplace=True)
#         """Quick Data Cleaning with CSV file"""
#         # zipcode_by_year.to_csv('zipcode_by_year.csv')
#         zipcode_by_year = pd.read_csv('zipcode_by_year.csv')
#         zipcode_by_year.plot()
#         plt.show()



        









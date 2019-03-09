import os
import requests
import json
import csv
import sqlite3
import urllib3
import sys
import pandas as pd
import pprint
from pandas.io.json import json_normalize
import psycopg2
urllib3.disable_warnings()
pd.options.display.max_seq_items

#Receiving API Data From Darksky in json format
http = urllib3.PoolManager()
r = http.request('GET',"https://api.darksky.net/forecast/ac89150eb898f7dda846b45ca4896211/37.8267,-122.4233",retries=3)
r.status
darkSkyData = json.loads(r.data.decode('utf-8'))

#Source File for top 50 most populated cities in the United States
cityDF = pd.read_csv('./foocity.csv')


#TODO: timestamp API pull

def getWeatherData():
    darkSkyList = []
    cityData = cityDF.to_dict('series')
    for (k,v), (k2,v2),(k3, v3), (k4, v4), (k5,v5) in zip(cityData['lat'].items(),cityData['lng'].items(),cityData['city'].items(),cityData['state_id'].items(),cityData['county_name'].items()):
        http = urllib3.PoolManager()
        r = http.request('GET',"https://api.darksky.net/forecast/ac89150eb898f7dda846b45ca4896211/{},{}?exclude=minutely,hourly,daily,alerts".format(v, v2),retries=3)
        data = json.loads(r.data.decode('utf-8'))
        data['city_id'] = v3
        data['state_id'] = v4
        data['county_name'] = v5
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
        darkSkyDF = darkSkyDF[['request_id','city_id','state_id', \
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


darkSkyDF = pd.DataFrame(getWeatherData())
print(darkSkyDF.head)
# print(darkSkyDF.shape)

#TODO: Figure out how to rename index & request_id
def appenddata():
        with open('/Users/ericrivetna/desktop/data analysis/DarkSkyDB1.csv') as inp, \
             open('/Users/ericrivetna/desktop/data analysis/DarkSkyDB.csv', 'a') as out:
             reader = csv.DictReader(inp)
             writer = csv.DictWriter(out, fieldnames=['','request_id','city_id','state_id','county_name','latitude','longitude','curr_time','curr_temperature','curr_apparentTemperature','curr_conditions','curr_expanded_summary','curr_precipIntensity','curr_dewPoint','curr_humidity','curr_precipProbability','curr_cloudCover','curr_windSpeed','curr_windGust','curr_windBearing','curr_visibility','curr_nearestStormDistance'])
             for row in reader:
                     if out.tell() == 0:
                             writer.writeheader()
                     writer.writerow(row)
                  
appenddata()


def postgres_SQL_creation():
        
        #TODO: Review Google for masking password from Source Code
        #Password is a garbage password unique to this Source Code
        con = psycopg2.connect(database="darksky", user="postgres",
                password="Cheesedog1", host="localhost",
                port=5432)
        con.autocommit = True
        cur = con.cursor()
        
        try:
                cur.execute('CREATE DATABASE DarkSky')
        except psycopg2.DatabaseError:
                print("Database Already Exists on Host/Port")
                
        
        create_geo_table = '''CREATE TABLE IF NOT EXISTS darksky_geo
         (request_id INT PRIMARY KEY NOT NULL UNIQUE,
         city_id TEXT NOT NULL,
         state_id TEXT NOT NULL,
         county_name TEXT NOT NULL,
         latitude FLOAT NOT NULL,
         longitude FLOAT NOT NULL);'''
         
        cur.execute(create_geo_table)
         
        create_weather_table = '''CREATE TABLE IF NOT EXISTS darksky_weather
         (request_id INT PRIMARY KEY NOT NULL UNIQUE,
         city_id TEXT NOT NULL,
         state_id TEXT NOT NULL,
         curr_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
         curr_temperature DECIMAL NOT NULL,
         curr_apparentTemperature DECIMAL NOT NULL,
         curr_conditions TEXT NOT NULL,
         curr_expanded_summary TEXT NOT NULL,
         curr_precipIntensity DECIMAL NOT NULL,
         curr_dewPoint DECIMAL NOT NULL,
         curr_humidity DECIMAL NOT NULL);'''
        
        cur.execute(create_weather_table)
        
        create_storm_table = '''CREATE TABLE IF NOT EXISTS darksky_storm
        (request_id INT PRIMARY KEY NOT NULL UNIQUE,
        city_id TEXT NOT NULL,
        state_id TEXT NOT NULL,
        curr_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        curr_cloudcover DECIMAL NOT NULL,
        curr_precipProbability DECIMAL NOT NULL,
        curr_windSpeed DECIMAL NOT NULL,
        curr_windGust DECIMAL NOT NULL,
        curr_windBearing INT NOT NULL,
        curr_visibility DECIMAL NOT NULL, 
        curr_nearestStormDistance INT NOT NULL);'''
        
        cur.execute(create_storm_table)

# postgres_SQL_creation()














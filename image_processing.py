#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-
"""
Created on Tue May 15 12:48:11 2019

@author: Anurag Sharma
"""

import numpy as np
from PIL import Image
import pandas as pd
import pickle
from datetime import datetime, timedelta
#from collections import Counter

# Import User Defined Class
from db import DB

class ImageProcessing:
    
    def remove_colors(self,image):
        pixels = [[0, 0, 0],[154, 173, 64],[100, 154, 64],[173, 173, 64],[173, 191, 64],
        [154, 154, 64],[191, 173, 64],[191, 191, 64],[100, 64, 64],[82, 64, 64],[173, 154, 64],
        [100, 100, 64],[100, 82, 64],[82, 154, 64],[154, 191, 64],[82, 100, 64],[191, 154, 64],
        [64, 82, 64],[64, 100, 64],[82, 82, 64],[154, 100, 64],[100, 191, 64],[173, 100, 64],
        [154, 82, 64],[173, 82, 64],[191, 100, 64],[82, 173, 64],[64, 173, 64],[212, 155, 95]]
        for combination in pixels:
            data = np.array(image)
            rgb = data[:,:,:3]
            original_green = [100, 173, 64] # Replacing with Default Green Color
            mask = np.all(rgb == combination, axis = -1)
            data[mask] = original_green
            image = Image.fromarray(data)
        
        return image
    
    
    def cloudcolor(self,image):
        clouds = image.getcolors()
        clouds = [x for x in clouds if x[1] != (100, 173, 64)]
        clouds = pd.DataFrame(dict(count=[x[0] for x in clouds],pixels=[x[1] for x in clouds]))
        clouds = clouds.reset_index(drop=True)
        color_coding = {7.6:(58, 0, 160),14:(0, 25, 176),21:(0, 58, 200),27:(0, 71, 255),34:(0, 121, 255),
                        41:(26, 163, 255),47:(83, 209, 255),54:(135, 241, 255),60:(255, 255, 255),
                        67:(252, 252, 122),74:(255, 230, 0),80:(255, 189, 0),87:(255, 115, 0),
                        93:(255, 63, 0),100:(200, 0, 0)}
        reflect = []
        for i in range(len(clouds)):
            x = [key for key, value in color_coding.items() if clouds.loc[i,'pixels'] == value]
            if len(x) > 0:
                reflect.append(x[0])
            else:
                reflect.append(0)
        
        clouds['rainfall'] = reflect
        clouds=clouds[clouds.rainfall>0]
        clouds.reset_index(drop=True,inplace=True)
        return clouds
    
    
    def pixelcoding(self,clouds,image):
        pixel = list(image.getdata())
        index = []
        for row in range(len(clouds)):
            indices = [i for i, x in enumerate(pixel) if x == clouds.loc[row,'pixels']]
            indices = [(x%480,x//480) for x in indices]
            index.append(indices)
        clouds['pixel_area'] = index
        return clouds
    
    def rainfallcoding(self,clouds):
        rain = {}
        for i in range(len(clouds)):
            rain[clouds.loc[i,'rainfall']] = clouds.loc[i,'pixel_area']
            
        return rain
    
    def check_neighbours(self,pixel,pixel_list):
        loki = []
        x,y = pixel[0], pixel[1]
        neighbours = [(x-1,y-1),(x-1,y),(x-1,y+1),(x,y-1),(x,y+1),(x+1,y-1),(x+1,y),(x+1,y+1)]
        for i in pixel_list:
            if (i[0],i[1]) in neighbours:
                loki.append(i)
                
        if len(loki) > 0:
            return loki
        else:
            return 0
    
    def group_clouds(self,clouds):
        pixel_list = [item for sublist in clouds.loc[:,'pixel_area'] for item in sublist]
        cloud_group= []
        while len(pixel_list) > 1:
            thanos = [pixel_list[0]]
            i = 0
            pixel_list.remove(thanos[0]) # remove head value
            while i < len(thanos):
                thor = self.check_neighbours(thanos[i], pixel_list)
                if thor:
                    for avengers in thor:
                        thanos.append(avengers)
                        pixel_list.remove(avengers)
                i=i+1
            cloud_group.append(thanos)
        cloud_group = [sublist for sublist in cloud_group if len(sublist) > 10]
        return cloud_group
    
    def get_centre(self,cloud_group):
        fury = []
        if len(cloud_group) > 0:
            for h in range(len(cloud_group)):
                ironman = cloud_group[h]
                jarvis = []
                ultron = []
                for j in range(len(ironman)):
                    jarvis.append(ironman[j][0])
                    ultron.append(ironman[j][1])
                jarvis = np.mean(jarvis)
                ultron = np.mean(ultron)
                fury.append((jarvis,ultron,len(ironman)))
            black_widow = sum(fury[g][0] * fury[g][2] for g in range(len(fury))) / sum([size[2] for size in fury])
            hawkeye = sum(fury[g][1] * fury[g][2] for g in range(len(fury))) / sum([size[2] for size in fury])
            quicksilver = (len(fury),sum([fury[g][2] for g in range(len(fury))]))
            vision = (black_widow,hawkeye,quicksilver)
        else:
            vision = np.nan
        return vision
    
    def dicttopickle(self, dictionary,file_name):
        with open(file_name, 'wb') as fp:
            pickle.dump(dictionary, fp, protocol=pickle.HIGHEST_PROTOCOL)
            
    def pickletodict(self, pickle_file):
        with open(pickle_file, 'rb') as fp:
            dictionary = pickle.load(fp)
            
        return dictionary

    def getting_windspeed(self,data,shifting,start,end):
        date_range = list(pd.date_range(start = (start - timedelta(minutes=30)), end = (end - timedelta(minutes=30)), freq = '15Min'))
        data['datetime_quarter'] = date_range
#        data['datetime_shift_ahead'] = data['datetime'].shift(-shifting)
#        data['datetime_quarter_ahead'] = data['datetime_quarter'].shift(-shifting)
        data = data[data['cloud_mean'].isnull() == False]
        data['datetime_shift'] = data['datetime'].shift(1)
        data['cloud_mean_shift'] = data['cloud_mean'].shift(1)
        data = data.iloc[1:,:]
        data['time_diff'] = list(map(lambda x,y: (x - y).seconds,data['datetime'],data['datetime_shift']))
#        data['time_diff_ahead'] = list(map(lambda x,y: (y-x ).seconds,data['datetime'],data['datetime_shift_ahead']))
        data['pixel_diff'] = list(map(lambda x,y: (x[0]-y[0],x[1]-y[1]) ,data['cloud_mean'],data['cloud_mean_shift']))
        data['cloud_size_diff'] = list(map(lambda x,y: (x[2][0]-y[2][0],x[2][1]-y[2][1]) ,data['cloud_mean'],data['cloud_mean_shift']))
        data['diff_distance'] = data['pixel_diff'].apply(lambda x: (x[0] * 5/12, x[1] *5/12))
        data['wind_speed'] = list(map(lambda x,y: (x[0]*1000/y,x[1]*1000/y),data['diff_distance'],data['time_diff']))
        return data
    
    def convert_actual_datetime(self,dictionary,data):
        groot = {}
        for i in dictionary.keys():
            try: 
                key = pd.Timestamp(data.set_index('datetime').loc[i,'datetime_quarter'].item())
                value = dictionary[i]
                groot[key] = value
            except:
                pass
            
        return groot
    
    def get_actual(self,nube,location_coordinates,raining):
        spiderman = {}
        x1,x2,y1,y2 = location_coordinates[0],location_coordinates[2], location_coordinates[1],location_coordinates[3]
        for i in list(nube.keys()):
            bucky = [item for sublist in nube[i] for item in sublist]
            captain = []
            for falcom in bucky:
                if x1 <= falcom[0] <= x2 and y1 <= falcom[1] <= y2:
                    for panther in list(raining[i].keys()):
                            if falcom in raining[i][panther]:
                                yuri = panther
                                falcom = (falcom[0],falcom[1],yuri)
                                captain.append(falcom)
            spiderman[i] = captain
            
        return spiderman
    
    def getting_forecast_clouds(self,data,groupofclouds,raining,h_time):
        data.reset_index(inplace = True,drop =True)
        hella = {}
        for row in range(len(data)):
            fecha,windspeed,time_diff = data.loc[row,'datetime_quarter'], data.loc[row,'wind_speed'], h_time
            if groupofclouds[fecha]:
                valkerie = []
                for element in range(len(groupofclouds[fecha])):
                    for atom in range(len(groupofclouds[fecha][element])):
                        for panther in list(raining[fecha].keys()):
                            if groupofclouds[fecha][element][atom] in raining[fecha][panther]:
                                yuri = panther
                                killmonger = ((-0.25 * (yuri - 7) /93) + 1) 
                                valkerie.append((groupofclouds[fecha][element][atom][0] + (windspeed[0]*killmonger*time_diff*12/5000), groupofclouds[fecha][element][atom][1] + (windspeed[1]*killmonger*time_diff*12/5000), yuri))     
            hella[fecha] = valkerie
        return hella
    
    def getting_forecast(self,forecasted_clouds,location_coordinates):
        spiderman = {}
        x1,x2,y1,y2 = location_coordinates[0],location_coordinates[2], location_coordinates[1],location_coordinates[3]
        for i in list(forecasted_clouds.keys()):
            bucky = forecasted_clouds[i]
            captain = []
            for falcom in bucky:
                if x1 <= falcom[0] <= x2 and y1 <= falcom[1] <= y2:
                    captain.append(falcom)
            spiderman[i] = captain
        
        return spiderman
    
    def converting_df(self, dictionary):
        rain_intensity = []
        for i in list(dictionary.keys()):
            if len(dictionary[i]) > 0:
                hella = []
                for j in range(len(dictionary[i])):
                    hella.append(dictionary[i][j][2])
                rain_intense = np.mean(hella)                
            else:
                rain_intense = 0
            rain_intensity.append(rain_intense)

        df = pd.DataFrame({'datetime': list(dictionary.keys()),'rain_intensity': rain_intensity})
        return df
        
    def calculating_accuracy(self,actual,forecast,data,start,end):
        actual_df = self.converting_df(actual)
        forecast_df = self.converting_df(forecast)
        actual_df['datetime'] = pd.to_datetime(actual_df['datetime'])
        actual_df = actual_df.sort_values('datetime')
        actual_df.reset_index(inplace=True,drop=True)
        date_range = list(pd.date_range(start = (start - timedelta(minutes=30)), end = (end - timedelta(minutes=30)), freq = '15Min'))[:-1]
        actual_df['datetime'] = date_range
        forecast_df['datetime'] = pd.to_datetime(forecast_df['datetime'])
        data = data[['datetime_quarter','datetime_quarter_ahead']]
        data = data.rename(columns = {'datetime_quarter':'datetime'})
        forecast_df = pd.merge(forecast_df,data, on='datetime',how='left')
        forecast_df['datetime'] = forecast_df['datetime_quarter_ahead']
        forecast_df = forecast_df.iloc[:,:2]
        df = pd.merge(actual_df,forecast_df[['datetime','rain_intensity']],on='datetime',how='outer')
        df = df.rename(columns={'rain_intensity_x':'rain_intensity_actual','rain_intensity_y':'rain_intensity_forecast'})
        df = df.fillna(0)
        df['mae'] = abs(df['rain_intensity_actual'] - df['rain_intensity_forecast'])/7
        df['actual'] = df['rain_intensity_actual'].apply(lambda x: 1 if x > 0 else 0)
        df['forecast'] = df['rain_intensity_forecast'].apply(lambda x: 1 if x > 0 else 0)
        df['accuracy'] = list(map(lambda x,y: 1 if x == y else 0, df['actual'] , df['forecast']))
        return df
    
    def forecast_db(self,forecast,h_time):
        forecast_df = self.converting_df(forecast)
        forecast_df['datetime'] = pd.to_datetime(forecast_df['datetime'])
        forecast_df['datetime'] = forecast_df['datetime'] + timedelta(minutes=(h_time/60))
        forecast_df['rain_probability'] = forecast_df['rain_intensity'].apply(lambda x: 1 if x > 7 else 0)
        forecast_df['created_at'] = datetime.now()
        forecast_df['date'] = pd.to_datetime(forecast_df['datetime']).dt.date
        forecast_df['time'] = pd.to_datetime(forecast_df['datetime']).dt.time
        forecast_df['image_quality']= 'good_image'
        forecast_df['horizon']= str(int((h_time/120)-30))
        forecast_df = forecast_df[['date','time','rain_intensity','rain_probability','created_at','image_quality','horizon']]
        return forecast_df  

    def store_forecast(self,df):
        db = DB()
        engine = db.getEngine()
        try:
            df.to_sql(con=engine,name = 'radar_data',if_exists="append",index=False)
        except:
            print("Error in DB store")
        return df
    
    def norain(self,data,start,end,h_time):
        data= data.tail(1)
        date_range = list(pd.date_range(start = (start - timedelta(minutes=30)), end = (end - timedelta(minutes=30)), freq = '15Min'))[-1]
        data['datetime_quarter'] = date_range
        data['datetime_quarter']= data['datetime_quarter'] + timedelta(minutes=(h_time/60))
        data['date'] = pd.to_datetime(data['datetime_quarter']).dt.date
        data['time'] = pd.to_datetime(data['datetime_quarter']).dt.time
        data['rain_intensity'] = 0
        data['rain_probability'] = 0
        data['created_at'] = datetime.now()
        data['image_quality']= 'good_image'
        data['horizon']= str(int((h_time/120)-30))
        data = data[['date','time','rain_intensity','rain_probability','created_at','image_quality','horizon']]
        return data
    
    def no_img(self,start,end,msg,h_time):
        date_range = list(pd.date_range(start = (start - timedelta(minutes=30)), end = (end - timedelta(minutes=30)), freq = '15Min'))[-1]
        temp= pd.DataFrame(columns=['datetime_quarter'])
        temp.loc[0,'datetime_quarter'] = date_range
        temp['datetime_quarter']= temp['datetime_quarter'] + timedelta(minutes=(h_time/60))
        temp['date'] = pd.to_datetime(temp['datetime_quarter']).dt.date
        temp['time'] = pd.to_datetime(temp['datetime_quarter']).dt.time
        temp['rain_intensity'] = np.nan
        temp['rain_probability'] = np.nan
        temp['created_at'] = datetime.now()
        temp['image_quality']= msg
        temp['horizon']= str(int((h_time/120)-30))
        temp = temp[['date','time','rain_intensity','rain_probability','created_at','image_quality','horizon']]
        return temp
        
        

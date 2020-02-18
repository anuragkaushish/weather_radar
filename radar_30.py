#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-
"""
Created on Fri May 18 16:43:05 2019

@author: Anurag Sharma
"""

import os
import sys
import pandas as pd
from PIL import Image
from datetime import datetime, timedelta
import pytesseract
import logging
import time
sys.path.append('/usr/local/lib/python3.5/dist-packages/pytesseract/')

#path1 = 'C:/Users/anurag/Documents/Python Scripts/RadarImages/server/'
path1='/home/sanand/radar/'
os.chdir(path1)

from image_processing import ImageProcessing
from image_grabber import ImageGrabber
processor = ImageProcessing()
grabber = ImageGrabber()

# Setting Logger
logger = logging.getLogger('radar_30')
hdlr = logging.FileHandler(path1 + '/logs/radar_30.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)
logger.info('Started')

# Setting Timezone
os.environ["TZ"] = "Asia/Kolkata"
time.tzset() 
logger.info('Timezone Set: {}'.format(datetime.now()))


start = datetime.now().replace(second=0, microsecond=0)
end =  start + timedelta(minutes=15)
seconds = (end - start).total_seconds()
minutes = []
step = timedelta(minutes=15)

for i in range(0, int(seconds), int(step.total_seconds())):
    minutes.append(start + timedelta(seconds=i))
    
data = pd.DataFrame(columns = ['datetime','cloud_mean'])
groupofclouds = {}
raining = {}

for minute in minutes:
    logger.info(minute)
    date = minute
    date_format = date.strftime('%Y-%m-%d-%H-%M')
    image_type = 'sri'
    image_name = '{}_dlh_{}'.format(image_type,date_format)
    image_name = image_name[:-2]
    
    ### Image Grabbing
    try:
        for number in range(11):
            image_name1 = image_name + datetime.strftime((pd.to_datetime(date).floor('15min') + timedelta(minutes=number)),"%M")
            try:
                grabber.server_to_server(image_type,image_name1)
                logger.info('Image: {0}_Grabbed_successfully'.format(image_name1))
                image_name = image_name1
                break
            except Exception as e:
                logger.info('Error while grabbing image: {0} _due to: {1}'.format(image_name1, str(e)))
                logger.info('Re-trying grabbing image:{0}'.format(image_name1))
                try:
                    grabber.server_to_server(image_type,image_name1)
                    image_name = image_name1
                    logger.info('On re-trying Image: {0}_Grabbed_successfully'.format(image_name1))
                    break
                except:
                    logger.info('Re-trying also failed_Error while grabbing image: {0} _due to: {1}'.format(image_name1, str(e)))
                    temp = processor.no_img(start,end,'no_image',3600)
                    processor.store_forecast(temp)
                    del temp
                    logger.info('no_image!')
    except:
         pass

    ### Image Loading and Pixelation to RGB
    try:
        original = Image.open("/home/sanand/radar/images/{}.gif".format(image_name)).convert('RGB')
        logger.info('Image: {0}_loaded_successfully'.format(image_name))
    except Exception as e:
        logger.info('Error for image: {0} _due to: {1}'.format(image_name, str(e)))
        logger.info('Re-trying loading image:{0}'.format(image_name))
        try:
            original = Image.open("/home/sanand/radar/images/{}.gif".format(image_name)).convert('RGB')
            logger.info('On re-trying Image: {0}_loaded_successfully'.format(image_name))
        except Exception as e:
            logger.info('Re-trying also failed_Error while loading image: {0} _due to: {1}'.format(image_name, str(e)))
            temp = processor.no_img(start,end,'bad_image',3600)
            processor.store_forecast(temp)
            del temp
            logger.info('bad_image!')
            pass
    
    ### Image Processing
    try:
        ### Cropping Image for time
        timing = original.crop((722, 220,853, 245))
        current_time = pytesseract.image_to_string(timing, config='-psm 6')
        current_time = datetime.strptime(current_time,'%H:%M:%S').time()
        
        ### Cropping image to have just the radar image
        img = original.crop((120, 120, 600, 600))
        logger.info('image cropped')
        
        ### Removing unnecessary colors
        img = processor.remove_colors(img)
        logger.info('color removed')
        
        ### Getting Color Coding in Dataframe
        clouds = processor.cloudcolor(img)
        logger.info('cloud color coding done')
        
        ### Getting location for each pixel
        clouds = processor.pixelcoding(clouds,img)
        logger.info('pixel coding done')
        
        ### Mapping Rainfall Intensity of each pixel
        rain = processor.rainfallcoding(clouds)
        logger.info('rainfall intensity done')
        
        ### Getting cloud information
        cloud_group = processor.group_clouds(clouds)
        logger.info('clouds formed')
        
        ### Getting the centre of the rain
        centre = processor.get_centre(cloud_group)
        logger.info('got weighted mean')
        
        df = pd.DataFrame(columns = ['datetime','cloud_mean'])
        if current_time < datetime(2018,1,1,18,30).time():
            df.loc[0,:] =  datetime.combine(date.date(),current_time),centre
        else:
            df.loc[0,:] =  datetime.combine((date.date()-timedelta(days=1)),current_time),centre
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['datetime'] = df['datetime'].apply(lambda x: x.tz_localize('UTC').tz_convert('Asia/Kolkata').tz_localize(None))
        groupofclouds[df.loc[0,'datetime'].strftime('%Y-%m-%d %H:%M')] = cloud_group
        raining[df.loc[0,'datetime'].strftime('%Y-%m-%d %H:%M')] = rain
        data = pd.concat([data,df])
        ### Uploading pickle as df
        try:
            bigdata = pd.read_pickle('data.pkl')
            bigraining = processor.pickletodict('raining.p')
            biggroupofclouds = processor.pickletodict('groupofclouds.p')
        except:
            bigdata = pd.DataFrame(columns = ['datetime','cloud_mean'])
            bigraining = {}
            biggroupofclouds = {}
        bigdata = pd.concat([bigdata,df])
        bigdata.sort_values('datetime',inplace=True)
        bigdata.drop_duplicates('datetime',inplace=True)
        bigdata.reset_index(drop=True,inplace=True)
        rained = {df.loc[0,'datetime'].strftime('%Y-%m-%d %H:%M'):rain}
        bigraining.update(rained)
        clouding = {df.loc[0,'datetime'].strftime('%Y-%m-%d %H:%M'):cloud_group}
        biggroupofclouds.update(clouding)
        # Saving New Values
        bigdata.to_pickle('data.pkl')
        processor.dicttopickle(bigraining,'raining.p')
        processor.dicttopickle(biggroupofclouds,'groupofclouds.p')
        logger.info('Data Saved till {}'.format(minute))        
    except Exception as e:
        logger.info('Error for image: {0} _while processing due to: {1}'.format(image_name, str(e)))
        df = pd.DataFrame(columns = ['datetime','cloud_mean'])
        current_time = (date - timedelta(minutes = 360)).time()
        if current_time < datetime(2018,1,1,18,30).time():
            df.loc[0,:] =  datetime.combine(date.date(),current_time),centre
        else:
            df.loc[0,:] =  datetime.combine((date.date()-timedelta(days=1)),current_time),centre
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['datetime'] = df['datetime'].apply(lambda x: x.tz_localize('UTC').tz_convert('Asia/Kolkata').tz_localize(None))
        clouding = {df.loc[0,'datetime'].strftime('%Y-%m-%d %H:%M'): []}
        rained = {df.loc[0,'datetime'].strftime('%Y-%m-%d %H:%M'): {}}
        data = pd.concat([data,df])
        bigdata = pd.read_pickle('data.pkl')
        bigraining = processor.pickletodict('raining.p')
        biggroupofclouds = processor.pickletodict('groupofclouds.p')
        bigdata = pd.concat([bigdata,df])
        bigdata.sort_values('datetime',inplace=True)
        bigdata.drop_duplicates('datetime',inplace=True)
        bigdata.reset_index(drop=True,inplace=True)
        bigraining.update(rained)
        biggroupofclouds.update(clouding)
        # Saving New Values
        bigdata.to_pickle('data.pkl')
        processor.dicttopickle(bigraining,'raining.p')
        processor.dicttopickle(biggroupofclouds,'groupofclouds.p')
        logger.info('Image not found but data Saved till {}'.format(minute))

#### Uploading historicakl data
data_temp = bigdata
data= data_temp.loc[data_temp.datetime.dt.date.isin([minute.date()])].dropna()
data.sort_values('datetime',inplace=True)
data= data.tail(2)
data.reset_index(drop=True,inplace=True)
logger.info('data: {}'.format(data))
raining= bigraining
groupofclouds = biggroupofclouds

if((len(data)>=2) & any(data.datetime.isin([minute]))):
    ### Getting windspeed
    data = processor.getting_windspeed(data=data,shifting=4,start=start,end=end)
    logger.info('Got windspeed')
    ### Making copy of clouds list
    nube = groupofclouds.copy()    
    ### Updated Dictionaries
    raining = processor.convert_actual_datetime(raining,data)
    groupofclouds = processor.convert_actual_datetime(groupofclouds,data)
    logger.info('Actual Datetime Coversion Done')
    ### Getting position of forecasted clouds
    forecasted_clouds = processor.getting_forecast_clouds(data,groupofclouds,raining,3600)
    logger.info('Got position of forested rain')
    ### Getting forecast
    forecast = processor.getting_forecast(forecasted_clouds,(190, 215,240, 265))
    logger.info('Got info of rain in Delhi')
    ### Getting final forecats
    final = processor.forecast_db(forecast,3600)
    logger.info('Got Final DB')
else:
    final = processor.norain(data,start,end,3600)
    processor.store_forecast(final)
    logger.info('No rain!')

exec(open('./radar_60.py').read())


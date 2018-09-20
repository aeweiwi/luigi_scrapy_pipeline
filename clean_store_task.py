# coding: utf-8

import os
import os.path
import subprocess
import datetime
import luigi
import pandas as pd
import matplotlib
from googletrans import Translator
from unidecode import unidecode
import numpy as np
from sqlalchemy import create_engine
from crawl_task import CrawlTask, CrawlShellTask
from luigi.contrib import sqla
from sqlalchemy import String, Integer, Float, Date
import math
from geopy.geocoders import Nominatim


# PYTHONPATH='.:./scrapy_shobiddak' luigi --module clean_store_task SQLATask --today 2018-09-01
class CleanTask(luigi.Task):
    today_str = luigi.Parameter()

    def make_it_arabic(self,x):
        try:
            return unidecode(x)
        except Exception as e:
            return np.nan

    def make_it_arabic_number(self,x):

        if pd.isna(x):
            return np.nan
        elif u'اول' in x:
            return 1
        elif u'ثاني' in x:
            return 2
        elif u'ثالث' in x:
            return 3
        elif u'راب' in x:
            return 4
        elif u'خامس' in x:
            return 5
        elif u'عشره' in x:
            return 10
        else:
            return self.make_it_arabic(x)

    def remove_punct(self,s):

        try:

            if pd.isna(s):
                return s

            if str(s).isnumeric():

                return s
            else:

                s = s.replace(',', ' ')
                s = s.replace('-', ' ')
                return s.strip()
        except Exception as e:
            return s

    def requires(self):
        return CrawlShellTask(today_str= self.today_str)

    def output(self):
        return luigi.LocalTarget('data_processed_{0}'.format(self.today_str))

    def find_long_lat(self, geolocator, item):
        item_full_address = item['city'] + u' ' + item['address']
        item_city_address = item['city']

        location = geolocator.geocode(item_full_address)
        if location is None:
            location = geolocator.geocode(item_city_address)
        return location.longitude, location.latitude


    def run(self):
        data =  pd.read_csv(self.input().path)

        geolocator = Nominatim(user_agent="specify_your_app_name_here")

        data['price'] = data['price'].str.extract('(\d+)').astype(float)

        data['car_year'] = data['car_year'].str.extract('(\d+)').astype(float)
        data['motor_power'] = data['motor_power'].astype(float)
        data['posted_on'] = pd.to_datetime(data['posted_on'])

        data['dist_reading'] = data['dist_reading'].str.extract('(\d+)')
        data['dist_reading'] = data['dist_reading'].apply(lambda x: self.make_it_arabic(x))
        data['dist_reading'] = pd.to_numeric(data['dist_reading'], errors='coerce')

        data['previous_owners'] = data['previous_owners'].apply(lambda x: self.make_it_arabic_number(x))
        data['previous_owners'] = pd.to_numeric(data['previous_owners'], errors='coerce')

        data['long'], data['lat'] = data.apply(lambda x: self.find_long_lat(geolocator, x))

        data['brand'], data['brand_model'] = data['car_model'].str.split(' ', 1).str
        data['brand'] = data['brand'].str.replace(u'^بي$', 'bmw')
        to_translate = ['payment_type', 'cert_type', 'glass_type', 'car_color',
                        'fuel_type', 'gear_type', 'car_origin', 'car_status', 'brand', 'brand_model']
        # "address", "announced_by", "car_color","car_seats",  "car_status"
        cols = ["address", "announced_by", "car_color", "car_model",  "car_origin",
                "car_seats", "car_status", "car_year",  "cert_type", "city",
                "dist_reading", "extras", "fuel_type", "gear_type", "glass_type",
                "motor_power","payment_type", "phone_num", "posted_on",
                "previous_owners", "price", "url", "valid_until", "brand", "brand_model"]

        geolocator = Nominatim(user_agent="specify_your_app_name_here")

        for col in cols:
            data[col] = data[col].apply(lambda x: self.remove_punct(x))

        for index in data.index.values:
            for col in to_translate:
                translator = Translator()
                val = str(data.iloc[index][col])
                eng_text = translator.translate(val, src="ar", dest="en").text
                data.at[index,col] = eng_text


        #data.fillna(None, inplace=True)
        data[cols].to_csv(self.output().path, index=False, sep='\t', header=False)



class SQLATask(sqla.CopyToTable):
    # columns defines the table schema, with each element corresponding
    # to a column in the format (args, kwargs) which will be sent to
    # the sqlalchemy.Column(*args, **kwargs)
    today = luigi.Parameter()


    columns = [
        (["address", String(256)], {}),
        (["announced_by", String(256)], {}),
        (["car_color", String(256)], {}),
        (["car_model", String(256)], {}),
        (["car_origin", String(256)], {}),
        (["car_seats", String(256)], {}),
        (["car_status", String(256)], {}),
        (["car_year", String], {}),
        (["cert_type", String(256)], {}),
        (["city", String(256)], {}),
        (["dist_reading", String(256)], {}),
        (["extras", String(256)], {}),
        (["fuel_type", String(256)], {}),
        (["gear_type", String(256)], {}),
        (["glass_type", String(256)], {}),
        (["motor_power", String(256)], {}),
        (["payment_type", String(256)], {}),
        (["phone_num", String(256)], {}),
        (["posted_on", String(256)], {}),
        (["previous_owners", String(256)], {}),
        (["price", String(256)], {}),
        (["url", String(256)], {}),
        (["valid_until", String(256)], {}),
        (["brand", String(256)], {}),
        (["brand_model", String(256)], {})
    ]


    connection_string = "sqlite:///mydb.db"  # in memory SQLite database
    table = "cars"  # name of the table to store data

    def requires(self):
        return CleanTask(today_str=self.today)

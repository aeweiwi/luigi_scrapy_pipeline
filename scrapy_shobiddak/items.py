# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
from scrapy.loader.processors import Join, MapCompose
import hashlib

def join_img_names(x):
    return '|'.join(x)

def extract_img_name(x):
    x =  x.replace('thumb_', '')
    x = 'https://shobiddak.com{}'.format(x)
    x = bytearray(x.strip(), 'utf-8')
    img_name = 'full/{0}.jpg'.format(hashlib.sha1(x).hexdigest())
    return img_name

def replace_img(x):
    x =  x.replace('thumb_', '')
    return 'https://shobiddak.com{}'.format(x)


class ShobiddakCar(Item):

    car_model = Field()
    car_year = Field()
    car_color = Field()
    fuel_type = Field()
    car_origin = Field()
    cert_type = Field()
    gear_type = Field()
    glass_type = Field()
    motor_power = Field()
    dist_reading = Field()
    car_seats = Field()
    price = Field()
    payment_type = Field()
    url= Field()
    car_status = Field()
    previous_owners = Field()
    extras = Field()
    announced_by = Field()
    city=Field()
    address = Field()
    phone_num = Field()
    posted_on = Field()
    valid_until = Field()
    images_names = Field(input_processor = MapCompose(extract_img_name),
                        output_processor= join_img_names)

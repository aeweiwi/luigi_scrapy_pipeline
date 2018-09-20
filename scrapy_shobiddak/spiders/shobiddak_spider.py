# coding: utf-8

from scrapy_shobiddak.items import ShobiddakCar
from scrapy.spiders import BaseSpider, CrawlSpider, Rule
from scrapy.selector import HtmlXPathSelector
from scrapy.loader import XPathItemLoader, ItemLoader
from scrapy.loader.processors import Join, MapCompose
from scrapy.linkextractors import LinkExtractor
import sqlalchemy as sa
import pandas as pd


class shobiddak(CrawlSpider):
    name = "scrapy_shobiddak"
    # allowed_domains = ["https://shobiddak.com"]
    start_urls = ['https://shobiddak.com/cars']

    rules = [
        Rule(LinkExtractor(allow=(), restrict_xpaths='//a[@class="next_page"]'),
             follow=True),
        Rule(LinkExtractor(allow=(), restrict_xpaths='//p[@class="section_title"]'),
             callback='parse_sub', follow=True)
    ]

    def __init__(self):
        super().__init__()

    item_fields = {'car_model': "//h1[@class='section_title']/text()",

                   # 'car_year': "//table/tbody/tr/td/h3[@class='section_title']/text()",
                   'car_year': "//h3[contains(text(),'موديل')]/text()",
                   'car_color': "//tr[td/text()='لون السيارة']/td[2]/text()",
                   'fuel_type': "//tr[td/text()='نوع الوقود']/td[2]/text()",
                   'car_origin': "//tr[td/text()='أصل السيارة']/td[2]/text()",
                   'cert_type': "//tr[td/text()='رخصة السيارة']/td[2]/text()",
                   'gear_type': "//tr[td/text()='نوع الجير']/td[2]/text()",
                   'glass_type': "//tr[td/text()='الزجاج']/td[2]/text()",
                   'motor_power': "//tr[td/text()='قوة الماتور']/td[2]/text()",
                   'dist_reading': "//tr[td/text()='عداد السيارة']/td[2]/text()",
                   'car_seats': "//tr[td/text()='عدد الركاب']/td[2]/text()",
                   'price': "//div[@class='post-price']/text()",
                   'payment_type': "//tr[td/text()='وسيلة الدفع']/td[2]/text()",
                   'car_status': "//tr[td/text()='معروضة']/td[2]/text()",
                   'previous_owners': "//tr[td/text()='أصحاب سابقون']/td[2]/text()",
                   'extras': "//tr[td/text()='إضافات']/td[2]/text()",
                   'announced_by': "//tr[contains(td, 'اسم المُعلن') and @class='list-row']/td[2]/text()",
                   'city': "//tr[td/text()='العنوان']/td[2]/a/text()",
                   'address': "//tr[td/text()='العنوان']/td[2]/text()",
                   'phone_num': "//tr[td/text()='رقم الهاتف']/td[2]/text()",
                   'posted_on': "//tr[td/text()='تاريخ نشر الإعلان']/td[2]/text()",
                   'valid_until': "//tr[td/text()='تاريخ إنتهاء الإعلان']/td[2]/text()",

    }


    def parse_sub(self, response):
        self.logger.info('----------SCRAPING------------')
        loader = ItemLoader(ShobiddakCar(), response=response)

        loader.default_input_processor = MapCompose(str.strip)

        loader.add_value('url', response.url)

        for field, xpath in self.item_fields.items():
            loader.add_xpath(field, xpath)


        yield loader.load_item()

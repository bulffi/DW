# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonMovieItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    movie_id = scrapy.Field()
    actors = scrapy.Field()
    directors = scrapy.Field()
    release_date = scrapy.Field()
    movie_type = scrapy.Field()
    movie_version = scrapy.Field()
    movie_title = scrapy.Field()


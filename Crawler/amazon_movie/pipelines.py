# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import datetime

import pyodbc
import sys


class AmazonMoviePipeline(object):
    def open_spider(self, spider):
        server = 'localhost'
        database = 'ETL_ass_1'
        username = 'zzj'
        password = 'zzjHH'
        self.cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        self.cursor = self.cnxn.cursor()
        self.file = open('G:\\movie\\amazon_movie\\amazon_movie\\db_error.csv', 'w')

    def close_spider(self, spider):
        self.file.close()
        self.cursor.close()
        self.cnxn.close()

    def process_item(self, item, spider):
        try:
            if item.get('release_date') is None:
                self.cursor.execute("""
                                    insert into movie_info(movie_id, movie_title, directors, release_date, movie_type, movie_version, actors)
                                    values (?,?,?,?,?,?,?)
                            """, [
                    item.get('movie_id')[0], item.get('movie_title')[0], item.get('directors')[0],
                    datetime.datetime.strptime('0001', '%Y'),
                    item.get('movie_type')[0], item.get('movie_version')[0], item.get('actors')[0]
                ])
                self.cnxn.commit()
                return item
            self.cursor.execute("""
                    insert into movie_info(movie_id, movie_title, directors, release_date, movie_type, movie_version, actors)
                    values (?,?,?,?,?,?,?)
            """, [
                item.get('movie_id')[0], item.get('movie_title')[0], item.get('directors')[0],
                item.get('release_date')[0],
                item.get('movie_type')[0], item.get('movie_version')[0], item.get('actors')[0]
            ])
            self.cnxn.commit()
        except pyodbc.IntegrityError:
            print('ERROR!!!!!! CAN NOT WRITE', sys.exc_info())
            self.file.write(item.get('movie_id')[0])
        return item

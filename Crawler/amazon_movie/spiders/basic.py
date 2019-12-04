# -*- coding: utf-8 -*-
import scrapy
import datetime
import csv
from scrapy.loader import ItemLoader
from amazon_movie import items


class BasicSpider(scrapy.Spider):
    name = 'basic'
    allowed_domains = ['www.amazon.com']
    # start_urls = ['https://www.amazon.com/gp/product/B0002JP58Q/', 'https://www.amazon.com/gp/product/B06XGTHYY6',
    #              'https://www.amazon.com/gp/product/B002OHDRF2/', 'https://www.amazon.com/gp/product/B000UGBOT0']
    server = 'localhost'
    database = 'ETL_ass_1'
    username = 'zzj'
    password = 'zzjHH'
    count = 0

    def __init__(self):
        super().__init__()
        print('I am called')
        with open('G:\\movie\\amazon_movie\\amazon_movie\\spiders\\select_distinct_product_id__from_movie_r.csv',
                  'r') as f:
            reader = csv.reader(f)
            list_of_id = list(reader)
        for i in range(len(list_of_id)):
            # print(list_of_id[i])
            list_of_id[i] = 'https://www.amazon.com/gp/product/{}/ref=atv_dp_mv_of_dp_0'.format(list_of_id[i][0])
        self.start_urls = list_of_id
        # self.start_urls = ['https://www.amazon.com/gp/product/B0019RXT7O/ref=atv_dp_mv_of_dp_0']

    def store(self, actor, director, date, category, version, title):
        pass

    def parse(self, response, **kwargs):
        # print(response.body)
        # print(response.request.headers)
        loader = ItemLoader(item=items.AmazonMovieItem(), response=response)
        movie_id = response.url[
                   len('https://www.amazon.com/gp/product/'):len('https://www.amazon.com/gp/product/B003UMW648')]
        actors = ''
        directors = ''
        release_date = ''
        movie_type = ''
        movie_version = ''
        movie_title = ''
        if response.xpath(
                '//span[@class="av-terms avu-full-width av-secondary"]//a/text()').extract() != ['Terms']:
            for i in range(1, 100):
                list_info_head = '//*[@id="detail-bullets"]/table//tr/td/div/ul/li[{}]/b/text()'.format(i)
                list_info_head = response.xpath(list_info_head).extract()
                print(list_info_head)
                # print(response.xpath('//*[@id="detail-bullets"]').extract())
                if not list_info_head:
                    break
                list_info_head = list_info_head[0].strip()
                # print(list_info_head)
                if 'Actors' in list_info_head:
                    actors = response.xpath(
                        '//*[@id="detail-bullets"]/table//tr/td/div/ul/li[{}]/a//text()'.format(i)).extract()
                    actors = ', '.join(actors)
                elif 'Directors' in list_info_head:
                    directors = response.xpath(
                        '//*[@id="detail-bullets"]/table//tr/td/div/ul/li[{}]/a//text()'.format(i)).extract()
                    directors = ', '.join(directors)
                elif 'Release Date' in list_info_head:
                    release_date = response.xpath(
                        '//*[@id="detail-bullets"]/table//tr/td/div/ul/li[{}]/text()'.format(i)).extract()
                    release_date = release_date[-1].strip()
                    release_date = datetime.datetime.strptime(release_date, '%B %d, %Y')

            movie_version = ', '.join(response.xpath('//*[@id="tmmSwatches"]/ul/li//a/span[1]/text()').extract())
            try:
                movie_type = response.xpath('//div[@id="wayfinding-breadcrumbs_container"]//li//a/text()').extract()[
                    -1].strip()
            except IndexError:
                print('ERROR!!!!!! NO TYPE', response.url)
                if kwargs.get('back_addr', False):
                    yield scrapy.Request(kwargs['back_addr'], self.parse, cb_kwargs={'follow': False})
                    return
                else:
                    movie_type = ''
            try:
                movie_title = response.xpath('//*[@id="productTitle"]/text()').extract()[0].strip()
            except IndexError:
                print('ERROR!!!!!!! NO TITLE', response.url)
                if kwargs.get('back_addr', False):
                    yield scrapy.Request(kwargs['back_addr'], self.parse, cb_kwargs={'follow': False})
                    return
                else:
                    movie_title = ''
        else:
            # if not response.xpath('(//div[@class="aiv-wrapper"]/div/div/a)[1]').extract() \
            #         or not kwargs.get('follow', True):
            movie_type = ', '.join(response.xpath(
                # _2vWb4y(new one)   _3RXp_N(old one)
                '(//div[@class="_2vWb4y dv-dp-node-meta-info"]//dl)[1]/dd/a/text()').extract())
            directors = ', '.join(response.xpath(
                '(//div[@class="_2vWb4y dv-dp-node-meta-info"]//dl)[2]/dd/a/text()').extract())
            actors = ', '.join(response.xpath(
                '(//div[@class="_2vWb4y dv-dp-node-meta-info"]//dl)[3]/dd/a/text()').extract())
            try:
                release_date = datetime.datetime.strptime(
                    response.xpath('//span[@data-automation-id="release-year-badge"]/text()').extract()[0],
                    '%Y'
                )
            except:
                print('ERROR!!!!!! PRIME NO DATE', response.url)
                release_date = None
            try:
                other_versions = ['Prime Video (streaming online video)']
                version_infos = response.xpath(
                    '//div[@class="aiv-container-limited dv-cross-linking-other-formats"]//div[@class="a-row '
                    'a-spacing-medium"]//div[@class="a-button-text"]/text()').extract()
                assert len(version_infos) % 3 == 0
                for i in range(len(version_infos) // 3):
                    other_versions.append(version_infos[i * 3].strip())
                movie_version = ', '.join(other_versions)
            except IndexError:
                print('ERROR!!!!! PRIME ON VERSION', response.url)
                movie_version = 'Prime Video (streaming online video)'
            try:
                  #  //div[@class="av-detail-section"]/div/h1/text()  new
                  #  //h1[@class="dv-node-dp-title avu-full-width UCX53S"]/text() old
                movie_title = \
                    response.xpath('//div[@class="av-detail-section"]/div/h1/text()').extract()[0]
            except IndexError:
                print('ERROR!!!! PRIME NO TITLE', response.url)
                movie_title = ''
            # normal_page = response.xpath('(//div[@class="aiv-wrapper"]/div/div/a)[1]').attrib['href']
            # if normal_page:
            #     normal_page = response.urljoin(normal_page)
            #     yield scrapy.Request(normal_page, self.parse, cb_kwargs={'back_addr': response.url})
            #     return

        if actors == '' and directors == '' and movie_title == '' and movie_version == '' and movie_type == '':
            yield scrapy.Request(response.url, callback=self.parse)
            return

        loader.add_value('movie_id', movie_id)
        loader.add_value('actors', actors)
        loader.add_value('directors', directors)
        loader.add_value('release_date', release_date)
        loader.add_value('movie_type', movie_type)
        loader.add_value('movie_version', movie_version)
        loader.add_value('movie_title', movie_title)
        # print(loader.load_item())
        yield loader.load_item()

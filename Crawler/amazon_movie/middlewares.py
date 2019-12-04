# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import requests
from scrapy import signals
import base64
import time
import hashlib

class AmazonMovieSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class AmazonMovieDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class ProxyMiddleware(object):
    # overwrite process request
    def process_request(self, request, spider):
        # 设置代理服务器域名和端口，注意，具体的域名要依据据开通账号时分配的而定
        # print('I am proxy')
        request.meta['proxy'] = "http-proxy-t3.dobel.cn:9180"

        # 设置账号密码
        proxy_user_pass = "QAZXSW7EBELQS70:sgThtJkQ"
        # setup basic authentication for the proxy
        # For python3
        encoded_user_pass = "Basic " + base64.urlsafe_b64encode(bytes((proxy_user_pass), "ascii")).decode("utf8")
        # For python2
        # encoded_user_pass = "Basic " + base64.b64encode(proxy_user_pass)

        request.headers['Proxy-Authorization'] = encoded_user_pass
        request.meta['max_retry_times'] = 10
        # # 设置账号密码
        # # proxy_user_pass = "forward.xdaili.cn:80"
        # ip = "forward.xdaili.cn"
        # port = "80"
        #
        # ip_port = ip + ":" + port
        # timestamp = str(int(time.time()))
        # orderno = 'ZF20199281982Csx0WT'
        # secret = 'a7ca1973e06d43fabeace9a746f61226'
        # string = "orderno=" + orderno + "," + "secret=" + secret + "," + "timestamp=" + timestamp
        # string = string.encode()
        # md5_string = hashlib.md5(string).hexdigest()
        # sign = md5_string.upper()
        # # print(sign)
        # auth = "sign=" + sign + "&" + "orderno=" + orderno + "&" + "timestamp=" + timestamp
        # # setup basic authentication for the proxy
        # # For python3
        # # encoded_user_pass = "Basic " + base64.urlsafe_b64encode(bytes((proxy_user_pass), "ascii")).decode("utf8")
        # # For python2
        # # encoded_user_pass = "Basic " + base64.b64encode(proxy_user_pass)

        # request.meta['proxy'] = "https://" + ip_port
        # request.headers['Proxy-Authorization'] = auth

        # proxy = {"http": "http://" + ip_port, "https": "https://" + ip_port}
        # headers = {"Proxy-Authorization": auth,
        #            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36"}
        # r = requests.get("http://2000019.ip138.com", headers=headers, proxies=proxy, verify=False,
        #                  allow_redirects=False)
        # r.encoding = 'utf8'
        # print(r.status_code)
        # print(r.text)
        # if r.status_code == 302 or r.status_code == 301:
        #     loc = r.headers['Location']
        #     print(loc)
        #     r = requests.get(loc, headers=headers, proxies=proxy, verify=False, allow_redirects=False)
        #     r.encoding = 'utf8'
        #     print(r.status_code)
        #     print(r.text)
import os

import scrapy
from redis import Redis

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http import HtmlResponse
from scrapy_splash import SplashRequest, SplashJsonResponse, SplashTextResponse

from gov.items import GovItem
from gov.utils import md5_encode, make_file_name, get_start_urls
from gov.settings import REDIS_HOST, REDIS_PORT, DATA_DIR, DOMAINS_LIST, REDIS_DUPLICATE
 


class GovSpider(CrawlSpider):
    name = 'gov'


    allowed_domains, start_urls = get_start_urls(DOMAINS_LIST)
    conn = Redis(host=REDIS_HOST, encoding='utf-8', port=REDIS_PORT)
    print('Starting redis...')

    rules = (
        Rule(
            LinkExtractor(), 
            callback='parse_item', 
            process_request='use_splash',
            follow=True
            ),
        )
        

    def is_crawled(self, url_content):
        if not self.conn:
            return -1
        else:
            content = md5_encode(url_content)
            ex = self.conn.sadd('keys', content)

            return ex

    def parse_item(self,response):
        if not REDIS_DUPLICATE:
            return self._real_parse_item(response)

        else:
            is_crawled_status = self.is_crawled(response.body)

            if is_crawled_status == -1:
                self.logger.warning('PLease check if redis is start!')
                return None
            elif is_crawled_status == 0:
                self.logger.info('数据没有进行更新')
            elif is_crawled_status == 1:
                return self._real_parse_item(response)


    def _real_parse_item(self, response):

        item = GovItem(
            domain_collection=None,
            html=None,
            pdf=[],
            xls=[],
            images=[],
            others=[]
        )
        # 1.保存html

        filename = make_file_name(response.url, 'html')
        item['html'] = filename

        domain = response.url.split('/')[2]
        item['domain_collection'] = md5_encode(domain)
        abpath = DATA_DIR + item['domain_collection']

        if not os.path.exists(abpath):  # 第一次创建文件夹

            os.makedirs(abpath)

        with open(abpath + '/' + filename, 'wb') as f:
            f.write(response.body)

        # 2.保存其他资源
        images = response.selector.xpath('//img/@src').extract()
        pdf = response.selector.xpath('//a/@href[contains(.,".pdf")]').extract()
        xls = response.selector.xpath('//a/@href[contains(.,".xls")]').extract()
        urls =  images + pdf + xls

        if urls:
            for url in urls:
                
                """
                url = response.urljoin(url)
                self.logger.info(url)
                yield scrapy.Request(
                    "http://localhost:8050/render.html?url=" + url,
                    callback=self.save_files, 
                    cb_kwargs=dict(item=item)
                )
                """
                yield response.follow(url, callback=self.save_files, cb_kwargs=dict(item=item))


    def save_files(self,response,item):

        self.logger.info("Saving files...")
        abpath = DATA_DIR + item['domain_collection']
        filename = md5_encode(response.url)+'.'+response.url.split('.')[-1]

        with open(abpath +'/'+ filename, 'wb') as f:
            f.write(response.body)
            self.logger.info('Files downloading...' +filename)

        if filename.endswith('.pdf'):
            item['pdf'].append(filename)
        elif filename.endswith('.xls'):
            item['xls'].append(filename)
        elif filename.endswith('png') or filename.endswith('jpg'):
            item['images'].append(filename)
        else:
            item['others'].append(filename)
        return item


    def _requests_to_follow(self, response):
        if not isinstance(
                response,
                (HtmlResponse, SplashJsonResponse, SplashTextResponse)):
            return
        seen = set()
        for n, rule in enumerate(self._rules):
            links = [lnk for lnk in rule.link_extractor.extract_links(response)
                     if lnk not in seen]
            if links and rule.process_links:
                links = rule.process_links(links)
            for link in links:
                seen.add(link)
                r = self._build_request(n, link)
                yield rule.process_request(r)

    def use_splash(self, request):

        return SplashRequest(
                            url=request.url, 
                            callback=self.parse_item,
                            args={
                                'wait': 0.5
                                }
                            )

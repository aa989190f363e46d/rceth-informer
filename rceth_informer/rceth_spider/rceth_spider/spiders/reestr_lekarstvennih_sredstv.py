import logging
import re
from base64 import b64decode, b64encode
from itertools import chain, count, islice
from pathlib import PurePosixPath
from random import shuffle
from string import ascii_lowercase, digits
from urllib.parse import parse_qsl, urlparse

import scrapy
from scrapy.http import FormRequest, Request

logger = logging.getLogger(__name__)


class ReestrLekarstvennihSredstvSpider(scrapy.Spider):
    name = 'drugs_spider'
    allowed_domains = ['rceth.by']

    custom_settings = {
        'STATS_CLASS': 'rceth_spider.statscollectors.RcethStatsCollector',
        'COOKIES_ENABLED': False,
        'ROBOTSTXT_OBEY': False,
        'ONCURRENT_REQUESTS': 8,
        }

    url = 'https://rceth.by'
    path = '/Refbank/reestr_lekarstvennih_sredstv/results'

    def __init__(self):
        super().__init__()
        self.items_control_counter = 0

        self.qsf_path = \
            '//form[@id="main"]/input[@id="QueryStringFind"]/@value'
        self.qsf_cleaner = re.compile(br'FProps\[[1-9]\].+?\[;\]')

        self.pages_count_finder = re.compile(br'(?<=FOpt.CPage\[=\])(\d+)')
        self.items_count_finder = re.compile(br'(?<=FOpt.CRec\[=\])(\d+)')

        self.file_chunk = 512
        self.file_parts_count = 3

    def start_requests(self):
        russ_alpha_len = 32
        russ_alpha = map(chr, islice(count(ord('а')), russ_alpha_len))
        letters = (
            russ_alpha,
            ascii_lowercase,
            digits,
            )
        seq = list(chain.from_iterable(letters))
        shuffle(seq)
        logger.info(
            f'Initial sequence:\n\t{"".join(seq)}',
            extra={'spider': self.name},
            )
        req_stack = []
        for letter in seq:
            req = self.build_start_request(letter, req_stack)
            req_stack.append(req)
        yield req_stack.pop(0)

    def build_start_request(self, letter, req_stack):
        formdata = self.get_params_template(letter)
        return FormRequest(
            f'{self.url}{self.path}', dont_filter=True,
            formdata=formdata,
            callback=self.parse_pagination,
            meta={
                'letter': letter,
                'req_stack': req_stack,
                })

    def parse_pagination(self, response):
        request = response.request

        qsf = b64decode(response.xpath(self.qsf_path).get())

        pages_count_match = next(self.pages_count_finder.finditer(qsf))
        pages_count = int(pages_count_match.group(0))

        items_count_match = next(self.items_count_finder.finditer(qsf))
        items_count = int(items_count_match.group(0))

        qsf = b64encode(self.qsf_cleaner.sub(b'', qsf))

        req_bdy = dict(parse_qsl(request.body.decode()))
        req_bdy['QueryStringFind'] = qsf.decode()
        req_bdy['IsPostBack'] = 'true'
        req_stack = request.meta['req_stack']
        for page_num in range(2, pages_count+1):
            req_bdy['ValueSubmit'] = str(page_num)
            logger.debug(req_bdy)
            req = request.replace(
                formdata=req_bdy,
                callback=self.parse_page,
                meta={'req_stack': req_stack},
                )
            req_stack.append(req)

        self.inc_items_control_counter(items_count)
        self.add_stats(response.meta['letter'], items_count)

        yield from self.parse_page(response)

    def add_stats(self, letter, items_count):
        stats = self.crawler.stats
        stats.add_letter(letter, items_count, spider=self)

    def inc_items_control_counter(self, delta=-1):
        self.items_control_counter += delta

    def parse_page(self, response):

        request = response.request
        req_stack = request.meta['req_stack']
        if req_stack:
            yield req_stack.pop(0)

        links_td_xpath = '//div[@class="table-view"]/table/tbody/tr/td[2]'
        for td in response.xpath(links_td_xpath):
            details_page_link = td.xpath('a/@href').get()
            item_id = self.get_last_url_path_part(details_page_link)
            meta = {'item_id': item_id}
            yield Request(
                f'{self.url}{details_page_link}',
                callback=self.parse_item,
                meta=meta,
                )

    def parse_item(self, response):
        self.inc_items_control_counter()
        drug_item = {'id': response.meta['item_id']}
        main_content = response.xpath('//div[@class="results"]')[0]
        main_table_row = main_content.xpath('div[@class="table-view"]/table')
        data_pairs = zip(
            main_table_row.xpath('thead/tr[1]/th'),
            main_table_row.xpath('tbody/tr[1]/td'),
            )
        main_props = []
        table_len = 9
        for pair in islice(data_pairs, table_len):
            main_props.append(tuple(map(
                lambda node: node.xpath('text()').get().strip(),
                pair,
                )))
        drug_item['props'] = main_props
        drug_forms_divs = main_content.xpath('div[@class="row-view"]')
        drug_forms = {}
        for idx, drug_form_div in enumerate(drug_forms_divs):
            drug_form_props = self.get_drug_forms_props(drug_form_div)
            drug_forms[idx] = drug_form_props
        drug_item['drug_forms'] = drug_forms

        drug_item['files'] = {}
        files_td = next(data_pairs)[1]
        files_req_stack = []
        file_req_meta = {
            'item': drug_item,
            'files_req_stack': files_req_stack,
            }
        for file_link in files_td.xpath('a'):
            file_href = file_link.xpath('@href').get()
            file_req_meta['used_for'] = file_link.xpath('text()').get()
            file_req = Request(
                f'{self.url}{file_href}',
                method='HEAD',
                callback=self.parse_file,
                meta=file_req_meta,
                )
            files_req_stack.append(file_req)
        if not files_req_stack:
            yield drug_item
            return None
        yield from iter(files_req_stack)

    @staticmethod
    def get_drug_forms_props(drug_form_div):
        drug_form_props = []
        table_name = drug_form_div.xpath('h4/text()').get()
        drug_form_name = table_name
        if drug_form_name:
            drug_form_props.append(
                ('Наименование формы', drug_form_name.strip()),
                )
        table_body = drug_form_div.xpath('table/tbody')
        data_pairs = zip(
            table_body.xpath('tr/td[1]/span/text()').getall(),
            table_body.xpath('tr/td[2]/text()').getall(),
            )
        table_len = 15
        for name, tbl_value in islice(data_pairs, table_len):
            drug_form_props.append((name.strip(), tbl_value.strip()))
        return drug_form_props

    def parse_file(self, response):
        length = int(response.headers[b'Content-Length'])
        file_url = response.url
        file_id = self.get_last_url_path_part(file_url)
        file_name = file_id
        drug_item = response.meta['item']
        files_req_stack = response.meta['files_req_stack']
        files = drug_item['files']
        files[file_id] = {'name': file_name, 'c_sum_data': []}
        meta = {'used_for': response.meta['used_for'],
                'item': drug_item,
                'file_id': file_name,
                'file_part_num': 0,
                'files_req_stack': files_req_stack}
        header = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': response.headers[b'Content-Type'],
            }
        if length <= self.file_parts_count * self.file_chunk:
            file_part_req = Request(
                file_url, meta=meta, headers=header,
                callback=self.parse_file_part,
                )
            files_req_stack.append(file_part_req)
            yield file_part_req
            return None
        header = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': response.headers[b'Content-Type']}
        middle_start = (length - self.file_chunk) // 2
        middle_end = middle_start + self.file_chunk-1
        for part, bounds in enumerate((
                f'0-{self.file_chunk-1}',
                f'{middle_start}-{middle_end}',
                f'-{self.file_chunk}',
                ), 1):
            header['Range'] = f'bytes={bounds}'
            meta['file_part_num'] = part
            file_part_req = Request(
                file_url, meta=meta, headers=header,
                dont_filter=True,
                callback=self.parse_file_part,
                )
            files_req_stack.append(file_part_req)
            yield file_part_req
        files_req_stack.remove(response.request)

    def parse_file_part(self, response):
        resp_meta = response.meta
        drug_item = resp_meta['item']
        files = drug_item['files']
        file_data = files[resp_meta['file_id']]
        file_part_num = resp_meta['file_part_num']
        file_data['c_sum_data'].append((file_part_num, response.body))
        files_req_stack = resp_meta['files_req_stack']
        files_req_stack.remove(response.request)
        if files_req_stack:
            return
        yield drug_item

    def closed(self, reason):
        counter = self.items_control_counter
        if counter != 0:
            logger.warning(str.join('\n', (
                'The spider maybe didn\'t get all items! ',
                f'**{counter}** elements left!',
                )),
                extra={'spider': self},
                )

    @staticmethod
    def get_params_template(letter):
        return {
            'IsPostBack': 'False',
            'PropSubmit': 'FOpt_PageN',
            'ValueSubmit': '',
            'FOpt.PageC': '100',
            'FOpt.OrderBy': 'N_LP',
            'FOpt.DirOrder': 'asc',
            'VFiles': 'False',
            'FOpt.VFiles': 'False',
            'FOpt.VEField1': 'False',
            'FProps[0].IsText': 'True',
            'FProps[0].Name': 'N_LP',
            'FProps[0].CritElems[0].Num': '1',
            'FProps[0].CritElems[0].Val': letter,
            'FProps[0].CritElems[0].Crit': 'Start',
            'FProps[0].CritElems[0].Excl': 'False',
        }

    @staticmethod
    def get_last_url_path_part(url):
        url_path = urlparse(url).path
        url_path_parts = PurePosixPath(url_path).parts
        return url_path_parts[-1]

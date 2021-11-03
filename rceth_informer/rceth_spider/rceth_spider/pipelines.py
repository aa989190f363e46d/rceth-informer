# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from sqlite3 import connect
from zlib import crc32

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class RcethSpiderPipeline:

    @staticmethod
    def build_check_sum(c_sum_data, size=32):
        c_sum = 0
        for shift, bin_data in c_sum_data:
            c_sum |= crc32(bin_data) << shift * size
        return c_sum

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            sqlite_uri=crawler.settings.get('SQLITE_URI', ':memory:'),
            sqlite_db=crawler.settings.get('SQLITE_DATABASE', 'drugs'),
        )

    def __init__(self, sqlite_uri, sqlite_db):
        self.sqlite_uri = sqlite_uri
        self.sqlite_db = sqlite_db

    def open_spider(self, spider):
        self.connection = connect(self.sqlite_uri)

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        id_, props, drug_forms, files = ItemAdapter(item).values()

        for _, fl_data in files.items():
            c_sum_data = fl_data.pop('c_sum_data')
            fl_data['c_sum'] = RcethSpiderPipeline.build_check_sum(c_sum_data)

        return {
            'id': id_,
            'props': props,
            'drug_forms': drug_forms,
            'files': files,
            }


class RcethSpiderDebugPipeline:

    def __init__(self):
        pass

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):
        raise DropItem
        id_, props, drug_forms, files = ItemAdapter(item).values()
        return {'id': id_}


class DBWriter(object):
    """docstring for DataWriter"""
    def __init__(self, sqlite_uri, sqlite_db):
        super(DataWriter, self).__init__()
        self.sqlite_uri = sqlite_uri
        self.sqlite_db = sqlite_db
        self.connection__ = connect(sqlite_uri)

    def open(self):
        pass

    def create_db(self):
        pass

"""
Scrapy extension for collecting scraping stats
"""
import logging
import pprint
from collections import defaultdict
from itertools import chain, islice, zip_longest
from math import ceil

from scrapy.statscollectors import MemoryStatsCollector

logger = logging.getLogger(__name__)


class RcethStatsCollector(MemoryStatsCollector):

    @staticmethod
    def build_table(x_axis, pivot, col_width=8):

        def col_fmt(col_val):
            return f'{col_val:>{col_width}}'

        def line_fmt(cols_vals):
            return ''.join(map(col_fmt, cols_vals))

        table = [line_fmt(x_axis)]
        table.append('-'*len(table[0]))
        lines = map(line_fmt, pivot)
        out = list(chain(table, lines))

        return out

    def __init__(self, crawler):
        super().__init__(crawler)
        self._letters = {}

    def add_letter(self, letter, items_count, spider):
        self._letters[letter] = items_count

    def close_spider(self, spider, reason):
        if self._dump:
            logger.info(str.join('', (
                'Dumping Scrapy stats:\n',
                pprint.pformat(self._stats),
                )),
                extra={'spider': spider},
                )
        self.report_letters(spider)
        self._persist_stats(self._stats, spider)

    def report_letters(self, spider):

        letter_stats = self._letters

        logger.info(
            f'Full rceth.by items count: {sum(letter_stats.values())}',
            extra={'spider': spider})

        inv_letters = defaultdict(list)
        for letter, count in letter_stats.items():
            _k = f'{letter} {count:> 3}' if count else f'{letter}'
            inv_letters[ceil(count/100)].append((count, letter, _k))
        x_axis = sorted(inv_letters)

        def get_nth(it, place_num=3):
            return next(islice(it, place_num-1, place_num))

        def sort_and_rotate(two_it):
            return zip(*sorted(two_it, reverse=True))

        sorted_counts = (sort_and_rotate(inv_letters[lttr]) for lttr in x_axis)
        aggregates = map(get_nth, sorted_counts)
        pivot = zip_longest(*aggregates, fillvalue='')

        out = self.build_table(x_axis, pivot)

        logger.info(str.join('', (
            'Dumping letters stats:\n',
            pprint.pformat(out),
            )),
            extra={'spider': spider},
            )

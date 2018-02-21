"""
Copyright 2018 Adam Snyder

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import csv
import os

from scraper.util import download_to_file

EXCHANGES = ['NASDAQ', 'NYSE', 'AMEX']

# we lazy load the metadata, caching the results in memory in this dict for subsequent requests
in_memory_symbol_metadata_by_symbol = None


def get_symbols():
    """get the complete list of stock ticker symbols across known exchanges"""
    return list(get_symbol_metadata_by_symbol().keys())


def get_metadata_for_symbol(symbol):
    """get a dictionary of information pertaining to a stock ticker symbol, according to nasdaq's screening API"""
    return get_symbol_metadata_by_symbol()[symbol]


def warm_up():
    """can be run prior to training in order to pre-download all necessary data"""
    for exchange in EXCHANGES:
        _maybe_download_exchange(exchange)


def get_symbol_metadata_by_symbol():
    """get metadata for all stock ticker symbols, keyed by symbol"""
    global in_memory_symbol_metadata_by_symbol
    if in_memory_symbol_metadata_by_symbol is None:
        in_memory_symbol_metadata_by_symbol = {}
        for exchange in EXCHANGES:
            _add_exchange_data(in_memory_symbol_metadata_by_symbol, exchange)
    return in_memory_symbol_metadata_by_symbol


def _add_exchange_data(data, exchange):
    for key, value in _get_data_for_exchange(exchange).items():
        if key in data:
            value = data[key]
        value[exchange] = True
        data[key] = value


def _get_data_for_exchange(exchange):
    csv_file = _maybe_download_exchange(exchange)
    with open(csv_file, 'r') as f:
        data = list(csv.DictReader(f))
    _strip_whitespaces_in_data(data)
    return {i['Symbol']: i for i in data}


def _maybe_download_exchange(exchange):
    current_dir = os.path.dirname(__file__)
    csv_file = os.path.join(current_dir, os.path.pardir, 'data', 'exchanges', '%s.csv' % exchange)
    download_url = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=%s&render=download' % exchange
    if not os.path.exists(csv_file):
        download_to_file(download_url, csv_file)
    return csv_file


def _strip_whitespaces_in_data(data):
    for item in data:
        for key in item:
            item[key] = item[key].strip()


if __name__ == '__main__':
    warm_up()

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
import json
import os
import time

from scraper.symbol_metadata import get_symbols
from scraper.util import download_to_file

# we lazy load the time series data, caching the results in memory in this dict for subsequent requests
in_memory_time_series_data_by_symbol = {}


def count_entries(symbol):
    """get a count of the number of days of historical data that exist for a given stock ticker symbol"""
    if symbol in in_memory_time_series_data_by_symbol:
        return len(in_memory_time_series_data_by_symbol[symbol])
    return _count_entries_from_file(symbol)


def _count_entries_from_file(symbol):
    def blocks(files):
        """custom file reader function that is appreciably faster than built-in methods"""
        while True:
            b = files.read(65536)
            if not b:
                break
            yield b

    csv_file = _maybe_download_symbol(symbol)
    with open(csv_file, 'r') as f:
        num_lines = sum(bl.count('\n') for bl in blocks(f))
        return num_lines - 1  # subtract the header line


def get_time_series_data(symbol):
    """get an array of historical daily time series data for a given stock ticker symbol"""
    if symbol not in in_memory_time_series_data_by_symbol:
        in_memory_time_series_data_by_symbol[symbol] = _get_time_series_data_from_file(symbol)
    return in_memory_time_series_data_by_symbol[symbol]


def _get_time_series_data_from_file(symbol):
    csv_file = _maybe_download_symbol(symbol)
    with open(csv_file, 'r') as f:
        data = list(csv.DictReader(f))
    _apply_data_types(data)
    return data


def warm_up():
    """can be run prior to training in order to pre-download all necessary data"""
    for symbol in get_symbols():
        _maybe_download_symbol(symbol)


def _apply_data_types(data):
    for item in data:
        item['open'] = float(item['open'])
        item['high'] = float(item['high'])
        item['low'] = float(item['low'])
        item['close'] = float(item['close'])
        item['volume'] = int(item['volume'])


def _maybe_download_symbol(symbol):
    current_dir = os.path.dirname(__file__)
    csv_file = os.path.join(current_dir, os.path.pardir, 'data', 'symbols', '%s.csv' % symbol)
    if not os.path.exists(csv_file):
        download_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&' + \
                       'symbol=%s&outputsize=full&apikey=%s&datatype=csv' % (symbol, os.environ['ALPHAVANTAGE_KEY'])
        for i in range(3, 9):
            if i == 8:
                print('Failed 5 times to download symbol %s. Exiting.' % symbol)
                exit(1)  # fail on final download attempt
            download_to_file(download_url, csv_file)
            with open(csv_file, 'r') as f:
                try:
                    contents_as_json = json.loads(f.read())
                except json.JSONDecodeError:
                    break
            # if the response contents was parsable JSON then something went wrong (it should be csv)
            if 'Error Message' in contents_as_json:
                # errors mean the symbol was not found, so we instead write an empty file
                with open(csv_file, 'w') as f:
                    print('Download failed with error: ' + contents_as_json['Error Message'])
                    print('Clearing downloaded file')
                    f.write('')
                    break
            if 'Information' in contents_as_json:
                # information means a throttling warning, so we back off
                time_to_sleep = 2 ** i
                print('API calls are being throttled. Will retry in %d seconds...' % time_to_sleep)
                time.sleep(time_to_sleep)
    return csv_file


if __name__ == '__main__':
    warm_up()

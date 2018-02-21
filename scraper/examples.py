import bisect

import numpy as np

from scraper.symbol_metadata import get_symbols, get_metadata_for_symbol
from scraper.symbol_time_series_data import count_entries, get_time_series_data

EXAMPLE_SIZE = 100  # equal to the number of data points that the Alpha Vantage API returns


def get_examples():
    """generator function to get randomly ordered samples of financial data"""
    symbols = get_symbols()
    symbol_start_indices, total_examples = _index_symbols(symbols)
    selection_order = np.arange(total_examples)
    np.random.shuffle(selection_order)
    for sample_index in selection_order:
        # use a binary search to determine which symbol to use given the sample_index
        start_index_index = bisect.bisect_left(symbol_start_indices, sample_index + 1) - 1
        start_index = symbol_start_indices[start_index_index]
        offset = sample_index - start_index
        symbol = symbols[start_index_index]
        time_series_data = get_time_series_data(symbol)
        next_day = time_series_data[offset]
        previous_days = time_series_data[offset + 1: offset + 1 + EXAMPLE_SIZE]
        previous_days.reverse()
        metadata = get_metadata_for_symbol(symbol)
        yield previous_days, next_day, metadata


def _index_symbols(symbols):
    """analyse the symbols to determine where examples can be extracted"""
    symbol_start_indices = []
    next_start_index = 0
    for symbol in symbols:
        entry_count = count_entries(symbol)
        if entry_count > EXAMPLE_SIZE:
            symbol_start_indices.append(next_start_index)
            next_start_index += entry_count - EXAMPLE_SIZE
    total_examples = next_start_index
    return symbol_start_indices, total_examples

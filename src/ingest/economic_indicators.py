
"""Fetch Statistics Canada key economic indicators"""
from statcan_client import get_data_from_vectors, get_series_info_from_vectors
from subsets_utils import save_raw_json

KEY_VECTORS = [
    41881485, 41881486,
    41690973, 41690914, 41690926, 41690943, 41690952,
    282733, 282735, 282736, 282734,
    1513259, 1513263, 1513267,
    20974,
    1558,
    34457, 36200,
]


def run():
    print("Fetching series metadata...")
    series_info = get_series_info_from_vectors(KEY_VECTORS)
    print(f"  Got metadata for {len(series_info)} series")

    print("Fetching time series data...")
    data_response = get_data_from_vectors(KEY_VECTORS, num_periods=500)
    print(f"  Got {len(data_response)} data responses")

    save_raw_json({
        "series_info": series_info,
        "data": data_response
    }, "economic_indicators")

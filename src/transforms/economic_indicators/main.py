"""Transform Statistics Canada key economic indicators."""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "statcan_economic_indicators"

METADATA = {
    "id": DATASET_ID,
    "title": "Statistics Canada Economic Indicators",
    "description": "Key economic indicators from Statistics Canada including GDP, employment, CPI, trade, and other macroeconomic series.",
    "column_descriptions": {
        "vector_id": "StatCan vector identifier",
        "ref_period": "Reference period",
        "ref_period_2": "Secondary reference period",
        "value": "Data value",
        "scalar_factor": "Scalar factor code",
        "decimals": "Number of decimal places",
        "status_code": "Data status code",
        "symbol_code": "Symbol code",
        "release_time": "Data release timestamp",
        "title_en": "Series title in English",
        "title_fr": "Series title in French",
        "product_id": "Product identifier",
        "coordinate": "Table coordinate",
        "frequency": "Data frequency code",
    }
}

SCHEMA = pa.schema([
    ('vector_id', pa.int64()),
    ('ref_period', pa.string()),
    ('ref_period_2', pa.string()),
    ('value', pa.float64()),
    ('scalar_factor', pa.int64()),
    ('decimals', pa.int64()),
    ('status_code', pa.int64()),
    ('symbol_code', pa.int64()),
    ('release_time', pa.string()),
    ('title_en', pa.string()),
    ('title_fr', pa.string()),
    ('product_id', pa.string()),
    ('coordinate', pa.string()),
    ('frequency', pa.int64()),
])


def run():
    """Transform Statistics Canada economic indicators."""
    raw = load_raw_json("economic_indicators")
    series_info = raw.get("series_info", [])
    data_response = raw.get("data", [])

    vector_metadata = {}
    for item in series_info:
        if item.get('status') == 'SUCCESS':
            obj = item.get('object', {})
            vector_id = obj.get('vectorId')
            if vector_id:
                vector_metadata[vector_id] = {
                    'title_en': obj.get('SeriesTitleEn'),
                    'title_fr': obj.get('SeriesTitleFr'),
                    'product_id': str(obj.get('productId', '')),
                    'coordinate': obj.get('coordinate'),
                    'frequency': obj.get('frequencyCode'),
                }

    records = []
    for item in data_response:
        if item.get('status') != 'SUCCESS':
            continue

        obj = item.get('object', {})
        vector_id = obj.get('vectorId')
        vector_data_points = obj.get('vectorDataPoint', [])
        metadata = vector_metadata.get(vector_id, {})

        for point in vector_data_points:
            records.append({
                'vector_id': vector_id,
                'ref_period': point.get('refPer'),
                'ref_period_2': point.get('refPer2'),
                'value': point.get('value'),
                'scalar_factor': point.get('scalarFactorCode'),
                'decimals': point.get('decimals'),
                'status_code': point.get('statusCode'),
                'symbol_code': point.get('symbolCode'),
                'release_time': point.get('releaseTime'),
                'title_en': metadata.get('title_en'),
                'title_fr': metadata.get('title_fr'),
                'product_id': metadata.get('product_id'),
                'coordinate': metadata.get('coordinate'),
                'frequency': metadata.get('frequency'),
            })

    if not records:
        raise ValueError("No economic indicator data found")

    print(f"  Transformed {len(records):,} data points")
    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID, mode="overwrite")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()

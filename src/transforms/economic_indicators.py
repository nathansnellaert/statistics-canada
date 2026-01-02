"""Transform Statistics Canada economic indicators."""
import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish, validate

DATASET_ID = "statcan_economic_indicators"

METADATA = {
    "id": DATASET_ID,
    "title": "Statistics Canada Key Economic Indicators",
    "description": "Key economic indicators from Statistics Canada including GDP, employment, inflation, and trade data. Time series data from selected vectors.",
    "column_descriptions": {
        "date": "Reference date (YYYY-MM or YYYY-MM-DD)",
        "vector_id": "Unique vector identifier for the time series",
        "product_id": "Product ID of the parent cube",
        "coordinate": "Coordinate within the cube",
        "series_title_en": "English title of the series",
        "series_title_fr": "French title of the series",
        "value": "Observation value",
        "scalar_factor": "Scalar factor applied to value",
        "symbol": "Symbol or status flag for the observation",
        "frequency": "Data frequency code",
        "uom": "Unit of measure",
    }
}


def test(table: pa.Table) -> None:
    """Validate economic indicators output."""
    validate(table, {
        "columns": {
            "date": "string",
            "vector_id": "int64",
            "product_id": "string",
            "coordinate": "string",
            "series_title_en": "string",
            "series_title_fr": "string",
            "value": "double",
            "scalar_factor": "string",
            "symbol": "string",
            "frequency": "string",
            "uom": "string",
        },
        "not_null": ["date", "vector_id"],
        "min_rows": 100,
    })


def run():
    """Transform economic indicators."""
    print("Transforming economic indicators...")
    raw = load_raw_json("economic_indicators")

    series_info = raw.get("series_info", [])
    data_responses = raw.get("data", [])

    # Build lookup for series metadata
    series_lookup = {}
    for item in series_info:
        obj = item.get("object", {})
        vector_id = obj.get("vectorId")
        if vector_id:
            series_lookup[vector_id] = obj

    records = []
    for response in data_responses:
        obj = response.get("object", {})
        vector_id = obj.get("vectorId")
        data_points = obj.get("vectorDataPoint", [])

        # Get series metadata
        series_meta = series_lookup.get(vector_id, {})

        for point in data_points:
            value = point.get("value")
            records.append({
                "date": point.get("refPer", ""),
                "vector_id": vector_id,
                "product_id": str(series_meta.get("productId", "")),
                "coordinate": series_meta.get("coordinate", ""),
                "series_title_en": series_meta.get("SeriesTitleEn", ""),
                "series_title_fr": series_meta.get("SeriesTitleFr", ""),
                "value": float(value) if value is not None else None,
                "scalar_factor": str(series_meta.get("scalarFactorCode", "")),
                "symbol": point.get("symbolCode", ""),
                "frequency": str(series_meta.get("frequencyCode", "")),
                "uom": str(series_meta.get("memberUomCode", "")),
            })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} data points")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()

"""Transform Statistics Canada cube catalogue."""
import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish, validate

DATASET_ID = "statcan_cubes"

METADATA = {
    "id": DATASET_ID,
    "title": "Statistics Canada Data Cubes",
    "description": "Catalogue of all available data cubes (tables) from Statistics Canada. Each cube represents a statistical table with data on various topics.",
    "column_descriptions": {
        "product_id": "Unique product identifier for the cube",
        "cansim_id": "Legacy CANSIM table identifier",
        "cube_title_en": "English title of the cube",
        "cube_title_fr": "French title of the cube",
        "archived": "Archive status ('1'=archived, '2'=discontinued)",
        "subject_code": "Subject classification code",
        "survey_code": "Survey code",
        "frequency_code": "Data release frequency code",
        "start_period": "Start period of data availability",
        "end_period": "End period of data availability",
        "release_time": "Timestamp of most recent data release",
    }
}


def test(table: pa.Table) -> None:
    """Validate cube catalogue output."""
    validate(table, {
        "columns": {
            "product_id": "string",
            "cansim_id": "string",
            "cube_title_en": "string",
            "cube_title_fr": "string",
            "archived": "string",
            "subject_code": "string",
            "survey_code": "string",
            "frequency_code": "int",
            "start_period": "string",
            "end_period": "string",
            "release_time": "string",
        },
        "not_null": ["product_id", "cube_title_en"],
        "unique": ["product_id"],
        "min_rows": 100,
    })


def run():
    """Transform cube catalogue."""
    print("Transforming cube catalogue...")
    raw = load_raw_json("cubes")

    records = []
    for cube in raw:
        records.append({
            "product_id": str(cube.get("productId", "")),
            "cansim_id": cube.get("cansimId", ""),
            "cube_title_en": cube.get("cubeTitleEn", ""),
            "cube_title_fr": cube.get("cubeTitleFr", ""),
            "archived": cube.get("archived"),
            "subject_code": str(cube.get("subjectCode", "")),
            "survey_code": str(cube.get("surveyCode", "")),
            "frequency_code": cube.get("frequencyCode"),
            "start_period": cube.get("cubeStartDate", ""),
            "end_period": cube.get("cubeEndDate", ""),
            "release_time": cube.get("releaseTime", ""),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} cubes")

    test(table)
    upload_data(table, DATASET_ID, mode="overwrite")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()

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
        "cube_title_en": "English title of the cube",
        "cube_title_fr": "French title of the cube",
        "archive_status_code": "Archive status code",
        "archive_status_en": "Archive status in English",
        "archive_status_fr": "Archive status in French",
        "subject_code": "Subject classification code",
        "survey_code": "Survey code",
        "frequency_code": "Data release frequency code",
        "start_period": "Start period of data availability",
        "end_period": "End period of data availability",
    }
}


def test(table: pa.Table) -> None:
    """Validate cube catalogue output."""
    validate(table, {
        "columns": {
            "product_id": "string",
            "cube_title_en": "string",
            "cube_title_fr": "string",
            "archive_status_code": "string",
            "archive_status_en": "string",
            "archive_status_fr": "string",
            "subject_code": "string",
            "survey_code": "string",
            "frequency_code": "string",
            "start_period": "string",
            "end_period": "string",
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
            "cube_title_en": cube.get("cubeTitleEn", ""),
            "cube_title_fr": cube.get("cubeTitleFr", ""),
            "archive_status_code": str(cube.get("archiveStatusCode", "")),
            "archive_status_en": cube.get("archiveStatusEn", ""),
            "archive_status_fr": cube.get("archiveStatusFr", ""),
            "subject_code": str(cube.get("subjectCode", "")),
            "survey_code": str(cube.get("surveyCode", "")),
            "frequency_code": str(cube.get("frequencyCode", "")),
            "start_period": cube.get("cubeStartDate", ""),
            "end_period": cube.get("cubeEndDate", ""),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} cubes")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()

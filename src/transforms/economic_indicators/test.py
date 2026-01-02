import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate Statistics Canada economic indicators output."""
    validate(table, {
        "columns": {
            "vector_id": "int",
            "ref_period": "string",
            "value": "double",
            "title_en": "string",
        },
        "not_null": ["vector_id", "ref_period"],
        "min_rows": 100,
    })

    vectors = set(table.column("vector_id").to_pylist())
    assert len(vectors) >= 5, f"Should have multiple vectors, got {len(vectors)}"

    print(f"  Validated {len(table):,} economic indicator records")

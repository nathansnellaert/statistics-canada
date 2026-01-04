"""Statistics Canada WDS API client with rate limiting."""

from ratelimit import limits, sleep_and_retry
from subsets_utils import get, post

BASE_URL = "https://www150.statcan.gc.ca/t1/wds/rest"


@sleep_and_retry
@limits(calls=20, period=1)
def rate_limited_get(endpoint, params=None):
    """Make a rate-limited GET request to Statistics Canada API."""
    url = f"{BASE_URL}/{endpoint}"
    response = get(url, params=params, timeout=60.0)
    return response


@sleep_and_retry
@limits(calls=20, period=1)
def rate_limited_post(endpoint, json_data=None):
    """Make a rate-limited POST request to Statistics Canada API."""
    url = f"{BASE_URL}/{endpoint}"
    response = post(url, json=json_data, timeout=60.0)
    return response


def get_all_cubes_lite():
    """
    Get list of all available data cubes (tables).

    Returns:
        List of cube metadata dicts
    """
    response = rate_limited_get('getAllCubesListLite')
    response.raise_for_status()
    return response.json()


def get_cube_metadata(product_ids):
    """
    Get detailed metadata for specific cubes.

    Args:
        product_ids: List of product IDs (e.g., ['10100001', '10100002'])

    Returns:
        List of cube metadata with dimensions
    """
    json_data = [{"productId": pid} for pid in product_ids]
    response = rate_limited_post('getCubeMetadata', json_data)
    response.raise_for_status()
    return response.json()


def get_data_from_vectors(vectors, num_periods=10):
    """
    Get data for specific vectors (time series).

    Args:
        vectors: List of vector IDs (e.g., [1234567, 7654321])
        num_periods: Number of latest periods to fetch

    Returns:
        List of data points
    """
    json_data = [{"vectorId": v, "latestN": num_periods} for v in vectors]
    response = rate_limited_post('getDataFromVectorsAndLatestNPeriods', json_data)
    response.raise_for_status()
    return response.json()


def get_series_info_from_vectors(vectors):
    """
    Get metadata for specific vectors.

    Args:
        vectors: List of vector IDs

    Returns:
        List of series metadata
    """
    json_data = [{"vectorId": v} for v in vectors]
    response = rate_limited_post('getSeriesInfoFromVector', json_data)
    response.raise_for_status()
    return response.json()


def get_changed_cube_list(date):
    """
    Get list of cubes changed on a specific date.

    Args:
        date: ISO date string (e.g., '2025-01-15')

    Returns:
        List of changed cube IDs
    """
    response = rate_limited_get(f'getChangedCubeList/{date}')
    response.raise_for_status()
    return response.json()

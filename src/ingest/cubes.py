
"""Fetch Statistics Canada cube catalogue"""
from utils import get_all_cubes_lite
from subsets_utils import save_raw_json


def run():
    print("Fetching cube catalogue...")
    cubes = get_all_cubes_lite()
    save_raw_json(cubes, "cubes")
    print(f"Saved {len(cubes):,} cubes")

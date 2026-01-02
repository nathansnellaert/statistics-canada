#!/usr/bin/env python3
import os

"""Statistics Canada Connector"""
import os
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

import argparse
from subsets_utils import validate_environment
from ingest import cubes as ingest_cubes
from ingest import economic_indicators as ingest_indicators
from transforms import cubes as transform_cubes
from transforms import economic_indicators as transform_indicators


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        ingest_cubes.run()
        ingest_indicators.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        transform_cubes.run()
        transform_indicators.run()


if __name__ == "__main__":
    main()

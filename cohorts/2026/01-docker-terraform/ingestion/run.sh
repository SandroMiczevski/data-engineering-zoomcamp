#!/bin/bash
# Exit immediately if a command exits with a non-zero status.
set -e

# Run the first script
uv run python ingest_green_tripdata.py

# Run the second script (this will only run after the first one completes successfully)
uv run python ingest_taxi_zone_lookup.py
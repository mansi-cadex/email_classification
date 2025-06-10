#!/bin/bash

# Set PostgreSQL environment variables
export PGHOST=34.46.113.148
export PGPORT=5432
export PGDATABASE=o2c_dev
export PGUSER=sanskar_gawande
export PGPASSWORD=sanskar_gawande

# Remove Python cache
find . -type d -name "__pycache__" -exec rm -rf {} +

# Run the test
python3 test_db.py 
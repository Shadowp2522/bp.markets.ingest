#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
===============================================================================
File:        extract.py
Author:      JP Ueberbach
Created:     2025-12-06
Description: Extract to parquet

This module provides functionality to extract, filter, transform, and export
financial time series data from Dukascopy CSV files into partitioned Parquet 
datasets using DuckDB. 

It supports:
- Reading CSV files with a predefined Dukascopy schema.
- Filtering data by a date range.
- Optional exclusion of the most recent open candle.
- Exporting data into Parquet format partitioned by symbol and year.
- Customizable compression format for Parquet output.
- Integration with multiprocessing workflows.

Functions:
- extract_symbol(task: Tuple[str, str, str, str, str, Dict[str, Any]]) -> bool
    Processes a single CSV file and writes the resulting data to Parquet.
- fork_extract(task: Tuple[str, str, str, str, str, Dict[str, Any]]) -> bool
    Wrapper for multiprocessing pool execution of `extract_symbol`.

Dependencies:
- duckdb
- pathlib
- datetime
- uuid
- typing
- os
"""

import duckdb
import os
import uuid
from pathlib import Path
from datetime import datetime
from typing import Tuple, Dict, Any

# Define the expected schema for Dukascopy data
DUKASCOPY_CSV_SCHEMA = {
    'time': 'TIMESTAMP',
    'open': 'DOUBLE',
    'high': 'DOUBLE',
    'low': 'DOUBLE',
    'close': 'DOUBLE',
    'volume': 'DOUBLE',
}

CSV_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'

def extract_symbol(task: Tuple[str, str, str, str, str, str, Dict[str, Any]]) -> bool:
    """
    Process a single Dukascopy CSV file and export it to Parquet or CSV using
    a single DuckDB COPY statement.

    This function performs the full extract–transform–load (ETL) pipeline for one
    (symbol, timeframe) input file:

    **Operations performed**
    - Reads the raw Dukascopy CSV using `read_csv_auto` with a predefined schema.
    - Applies a timestamp filter: `[after_str, until_str)`.
    - Optionally excludes the final timestamp if `modifier == "skiplast"`.
    - Injects metadata columns: `symbol`, `timeframe`, and `year`.
    - Normalizes the timestamp column to DuckDB's TIMESTAMP type.
    - Writes output in a *single COPY command* to:
        • A partitioned Parquet dataset (`symbol` / `year`), or  
        • A partitioned CSV directory (`symbol` / `year`), or  
        • A single unpartitioned CSV file per worker.

    **Parameters**
    - `task`: 7-tuple containing:
        1. `symbol` (str) — Asset symbol (e.g., `"EURUSD"`).
        2. `timeframe` (str) — Bar interval (e.g., `"1m"`).
        3. `input_filepath` (str) — Path to the raw Dukascopy CSV file.
        4. `after_str` (str) — Start time (inclusive), formatted as a timestamp.
        5. `until_str` (str) — End time (exclusive), formatted as a timestamp.
        6. `modifier` (str) — Optional processing flag (currently `"skiplast"`).
        7. `options` (dict) — Export configuration:
            - `output_type`: `"parquet"` (default) or `"csv"`.
            - `partition`: Whether to partition CSV output.
            - `compression`: Compression codec (default `"zstd"`).
            - `output_dir`: Root directory for exported data.
            - `dry_run`: If true, prints planned operation and exits.

    **Returns**
    - `True` if the export succeeded.
    - `False` if `dry_run=True` and execution was skipped.

    **Notes**
    - All DuckDB work is executed inside an in-memory temporary database.
    - The function writes directly to disk using DuckDB `COPY ... TO` and
      does not load data into Python memory.
    - Parquet output is *always partitioned* by `(symbol, year)`.
    """

    # Unpack task parameters
    symbol, timeframe, input_filepath, after_str, until_str, modifier, options = task

    # Normalize output configuration
    output_type = options.get('output_type', 'parquet').upper()
    is_partitioned = options.get('partition', False)
    compression = options.get('compression', 'zstd').upper()
    
    # Dry-run mode prints planned execution and exits early
    if options['dry_run']:
        print(f"DRY-RUN: {symbol}/{timeframe} => {input_filepath} (mode: {output_type}, modifier: {modifier})")
        return False

    # Determine root output directory and ensure it exists
    root_output_dir = options.get('output_dir', f'./data/temp/{output_type.lower()}')
    Path(root_output_dir).mkdir(parents=True, exist_ok=True)

    # Select output path and COPY-format settings based on output type
    if output_type == 'PARQUET':
        # Always partitioned (symbol/year)
        output_path = root_output_dir
        format_options = f"""
            FORMAT PARQUET,
            PARTITION_BY (symbol, year),
            FILENAME_PATTERN 'part_{{uuid}}',
            COMPRESSION '{compression}',
            OVERWRITE_OR_IGNORE
        """
        
    elif output_type == 'CSV':
        if is_partitioned:
            # Partitioned CSV output
            output_path = root_output_dir
            format_options = f"""
                FORMAT CSV,
                PARTITION_BY (symbol, year),
                FILENAME_PATTERN 'part_{{uuid}}.csv',
                COMPRESSION '{compression}',
                HEADER true,
                DELIMITER ',',
                OVERWRITE_OR_IGNORE
            """
        else:
            # Each worker writes its own temp CSV file
            output_path = str(Path(root_output_dir) / f"{symbol}_{timeframe}_{uuid.uuid4()}.csv")
            format_options = f"""
                FORMAT CSV,
                COMPRESSION '{compression}',
                HEADER true,
                DELIMITER ',',
                OVERWRITE_OR_IGNORE
            """
    else:
        raise ValueError(f"Unsupported output type: {output_type}")

    # Column selection + computed fields inserted into the output
    select_columns = f"""
        '{symbol}'::VARCHAR AS symbol,
        '{timeframe}'::VARCHAR AS timeframe,
        CAST(strftime(Time, '%Y') AS VARCHAR) AS year, 
        strptime(CAST(Time AS VARCHAR), '{CSV_TIMESTAMP_FORMAT}') AS time, 
        open, high, low, close, volume
    """

    # Raw time column from the Dukascopy schema (first column)
    time_column_name = list(DUKASCOPY_CSV_SCHEMA.keys())[0]

    # Basic time filter window
    where_clause = f"""
        WHERE {time_column_name} >= TIMESTAMP '{after_str}'
          AND {time_column_name} < TIMESTAMP '{until_str}'
    """

    # Optional modifier: skip the latest timestamp from the CSV
    if modifier == "skiplast":
        where_clause += (
            f" AND {time_column_name} < (SELECT MAX({time_column_name}) "
            f"FROM read_csv_auto('{input_filepath}'))"
        )

    # DuckDB read_csv_auto call with predefined schema
    read_csv_sql = f"""
        SELECT *
        FROM read_csv_auto(
            '{input_filepath}',
            columns={DUKASCOPY_CSV_SCHEMA}
        )
    """

    # Final COPY statement that reads, filters, transforms, and exports in one pass
    copy_sql = f"""
        COPY (
            SELECT {select_columns}
            FROM ({read_csv_sql})
            {where_clause}
        )
        TO '{output_path}' 
        (
            {format_options}
        );
    """
    
    # Execute COPY in an isolated in-memory DuckDB session
    con = duckdb.connect(database=':memory:')
    con.execute(copy_sql)
    con.close()

    return True



# --- Wrapper for multiprocessing (required by run.py) ---
def fork_extract(task: Tuple[str, str, str, str, str, str, Dict[str, Any]]) -> bool:
    """Wrapper function for multiprocessing pool."""
    return extract_symbol(task)

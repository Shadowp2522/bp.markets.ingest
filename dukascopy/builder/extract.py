#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# extract.py

import duckdb
import os
import uuid
from datetime import datetime
from typing import Tuple, Dict, Any

# Define the expected schema for Dukascopy data
DUKASCOPY_CSV_SCHEMA = {
    'Time': 'TIMESTAMP',
    'Open': 'DOUBLE',
    'High': 'DOUBLE',
    'Low': 'DOUBLE',
    'Close': 'DOUBLE',
    'Volume': 'DOUBLE',
}

def extract_symbol(task: Tuple[str, str, str, str, str, Dict[str, Any]]) -> bool:
    """
    Extracts, filters, transforms, and exports a single CSV file to a 
    partitioned Parquet dataset using a single DuckDB COPY statement.

    Args:
        task: Tuple containing (symbol, timeframe, filepath, after_str, until_str, options_dict).
              Note: 'after' and 'until' are passed as strings to be process-safe.
              
    Returns:
        bool: True on successful execution. Raises Exception on failure.
    """
    symbol, timeframe, input_filepath, after_str, until_str, options = task
    
    root_output_dir = options.get('output_dir', './temp/parquet') 
    
    select_columns = f"""
        '{symbol}'::VARCHAR AS symbol,
        '{timeframe}'::VARCHAR AS timeframe,
        CAST(strftime(Time, '%Y') AS VARCHAR) AS year,
        Time,
        Open,
        High,
        Low,
        Close,
        Volume
    """
    
    time_column_name = list(DUKASCOPY_CSV_SCHEMA.keys())[0]

    where_clause = f"""
        WHERE {time_column_name} >= TIMESTAMP '{after_str}'
          AND {time_column_name} < TIMESTAMP '{until_str}'
    """
    
    if options.get('omit_open_candles'):
        where_clause += f" AND {time_column_name} < (SELECT MAX({time_column_name}) FROM read_csv_auto('{input_filepath}'))"
    
    read_csv_sql = f"""
        SELECT *
        FROM read_csv_auto(
            '{input_filepath}',
            columns={DUKASCOPY_CSV_SCHEMA}
        )
    """

    copy_sql = f"""
        COPY (
            SELECT {select_columns}
            FROM ({read_csv_sql})
            {where_clause}
        )
        TO '{root_output_dir}' 
        (
            FORMAT PARQUET,
            PARTITION_BY (symbol, year),
            FILENAME_PATTERN 'part_{{uuid}}',
            COMPRESSION '{options.get('compression', 'zstd').upper()}',
            OVERWRITE_OR_IGNORE
        );
    """

    con = duckdb.connect(database=':memory:')
    con.execute(copy_sql)
    con.close()
    
    return True

# --- Wrapper for multiprocessing (required by run.py) ---
def fork_extract(task: Tuple[str, str, str, str, str, Dict[str, Any]]) -> bool:
    """Wrapper function for multiprocessing pool."""
    return extract_symbol(task)

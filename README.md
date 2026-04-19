# Data Profiler Tool

## Overview
A configurable data profiling tool that scans database tables and extracts metadata and statistics.

## Features
- Supports DuckDB (TPC-DS dataset)
- Column-level statistics (min, max, distinct count)
- Sampling support
- Parallel processing
- Resume capability

## Setup

```bash
python -m venv profiler_env
source profiler_env/bin/activate
pip install -r requirements.txt

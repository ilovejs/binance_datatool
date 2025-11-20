# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BHDS (Binance Historical Data Service) downloads and maintains cryptocurrency market data from Binance AWS archives. It uses aria2 for parallel downloads, Polars for data processing, and stores data in Parquet format. The project has two main packages:
- `bhds`: CLI and core services (src/bhds/)
- `bdt_common`: shared utilities (src/bdt_common/)

## Required Commands

### Setup
```bash
uv sync && source .venv/bin/activate
```

### Format Code
```bash
uv run black . && uv run isort .
```

### CLI Usage
```bash
# Download historical data from Binance AWS
uv run bhds aws-download configs/download/spot_kline.yaml

# Parse downloaded CSV to Parquet
uv run bhds parse-aws-data configs/parsing/spot_kline.yaml

# Generate holistic 1m klines (with VWAP & funding)
uv run bhds holo-1m-kline configs/holo_1m/spot.yaml

# Resample to higher timeframes
uv run bhds resample configs/resample/spot.yaml
```

### Testing
```bash
# Run individual test scripts
uv run tests/aws_client.py
uv run tests/aws_downloader.py
```

### Library Usage
```bash
uv run python examples/kline_download_task.py /path/to/data
uv run python examples/cm_futures_holo.py /path/to/data
```

## Environment

- **Python ≥ 3.12** required for modern type hints
- **[uv](https://docs.astral.sh/uv/)** for package management
- **[aria2](https://aria2.github.io/)** for parallel downloads

### Environment Variables
- `BHDS_HOME`: BHDS data directory (default: `~/crypto_data/bhds`)
- `HTTP_PROXY`: Optional proxy for downloads
- `CRYPTO_BASE_DIR`: Used by test scripts (defaults to `~/crypto_data`)

## Architecture

### CLI Architecture
The CLI (`bhds.cli`) uses Typer and follows a task-based pattern. Each command loads a YAML config and runs a task class:
- `aws-download` → `AwsDownloadTask` (src/bhds/tasks/aws_download.py)
- `parse-aws-data` → `ParseAwsDataTask` (src/bhds/tasks/parse_aws_data.py)
- `holo-1m-kline` → `GenHolo1mKlineTask` (src/bhds/tasks/holo_1m_kline.py)
- `resample` → `HoloResampleTask` (src/bhds/tasks/holo_resample.py)

Task classes follow a common pattern defined in `bhds.tasks.common`:
1. Load YAML config via `load_config()`
2. Get BHDS home directory via `get_bhds_home()`
3. Create symbol filters via `create_symbol_filter_from_config()`
4. Implement async or sync `run()` method

### Data Flow
```
AWS Archives (zip) → aws_data/
    ↓ parse-aws-data
Parsed CSV → parsed_data/ (parquet)
    ↓ holo-1m-kline
Holistic 1m klines → holo_1m_klines/ (parquet with VWAP & funding)
    ↓ resample
Higher timeframes → resampled_klines/ (parquet)
```

### AWS Integration
- `bhds.aws.client.AwsClient`: Fetches file listings from Binance AWS S3
- `bhds.aws.downloader.AwsDownloader`: Downloads files via aria2
- `bhds.aws.path_builder`: Constructs AWS paths for different data types

Path builders generate URLs like:
- Klines: `data.binance.vision/data/{trade_type}/{data_freq}/klines/{symbol}/{time_interval}/`
- Funding rates: `data.binance.vision/data/{trade_type}/{data_freq}/fundingRate/{symbol}/`

### Polars Processing Patterns
- **Always use Lazy API**: Work with `pl.LazyFrame` for query optimization
- **Batch execution**: Use `execute_polars_batch()` from `bdt_common.polars_utils` to execute LazyFrame lists in batches with progress tracking
- **Multiprocessing**: Use `polars_mp_env()` as ProcessPoolExecutor initializer to limit threads per process

Example pattern:
```python
tasks = [lf.sink_parquet(path) for lf, path in lazy_frames_and_paths]
execute_polars_batch(tasks, desc="Writing parquet files", batch_size=32)
```

## Code Standards

- **Logging**: Always use `logger` from `bdt_common.log_kit`, never print()
- **Language**: English for all code, comments, and logs
- **Type hints**: Required; Python ≥ 3.12 syntax
- **Enums**: Use `bdt_common.enums` for TradeType, DataFrequency, ContractType, etc.
- **Config loading**: Use `bhds.tasks.common.load_config()` for YAML configs
- **Keep diffs small**: Make focused, reviewable changes
- **Run tests**: Execute at least one test script before committing

## Critical Files

- `@docs/ARCHITECTURE.md` – Project structure overview
- `@configs/CLAUDE.md` – YAML config fields & usage
- `@tests/CLAUDE.md` – Test catalog & how to run tests
- `@examples/CLAUDE.md` – Library usage patterns
- `src/bhds/cli.py` – CLI entry point
- `src/bhds/tasks/common.py` – Shared task utilities
- `src/bdt_common/polars_utils.py` – Polars batch execution helpers

## Constraints

- Never bypass git hooks (`--no-verify`)
- Never commit large unrelated changes
- Always format code before committing (black + isort)
- Test changes with at least one test script

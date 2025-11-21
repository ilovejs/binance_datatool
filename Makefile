.PHONY: help setup download verify clean format test

# Configuration
CONFIG_FILE ?= configs/download/spot_1h.yaml

help:
	@echo "BHDS - Binance Historical Data Service"
	@echo ""
	@echo "Available targets:"
	@echo "  make setup              - Install dependencies and activate virtualenv"
	@echo "  make download           - Download data (only missing files)"
	@echo "  make download-verify    - Download data and verify checksums"
	@echo "  make verify-only        - Verify checksums of existing files"
	@echo "  make redownload-failed  - Redownload only failed/invalid files"
	@echo "  make clear-failed       - Clear failed files tracker"
	@echo "  make format             - Format code with black and isort"
	@echo "  make test               - Run test scripts"
	@echo "  make clean              - Clean Python cache files"
	@echo ""
	@echo "Configuration:"
	@echo "  CONFIG_FILE=${CONFIG_FILE}"
	@echo ""
	@echo "Workflow:"
	@echo "  1. make setup              # First time setup"
	@echo "  2. make download-verify    # Download and verify all files"
	@echo "  3. make download-verify    # Auto-retries failed files from previous run"
	@echo ""
	@echo "How it works:"
	@echo "  - Failed files are tracked in .failed_files.json"
	@echo "  - Next run automatically retries ONLY failed files"
	@echo "  - Both data files AND checksums are redownloaded"
	@echo "  - Successfully verified files are removed from tracker"
	@echo ""
	@echo "Performance optimization:"
	@echo "  - Set retry_only=true in config to skip symbol listing"
	@echo "  - This avoids 575 AWS API calls when only retrying"
	@echo "  - Use for pure retry scenarios (no new files expected)"

setup:
	@echo "🔧 Setting up BHDS environment..."
	uv sync
	@echo "✅ Setup complete! Run: source .venv/bin/activate"

download:
	@echo "📥 Downloading data (without verification)..."
	@echo "Config: ${CONFIG_FILE}"
	uv run bhds aws-download ${CONFIG_FILE}

download-verify:
	@echo "📥 Downloading data with verification..."
	@echo "Config: ${CONFIG_FILE}"
	@echo "Note: Invalid files will be deleted and need redownload"
	uv run bhds aws-download ${CONFIG_FILE}

verify-only:
	@echo "🔍 Verifying existing files..."
	@echo "Config: ${CONFIG_FILE}"
	@echo "Note: This will delete invalid files if delete_mismatch=true"
	uv run bhds aws-download ${CONFIG_FILE}

redownload-failed:
	@echo "🔄 Redownloading failed/invalid files..."
	@echo "Config: ${CONFIG_FILE}"
	@echo ""
	@echo "How it works:"
	@echo "  1. Reads failed files list from .failed_files.json"
	@echo "  2. Downloads ONLY those files (data + checksums)"
	@echo "  3. Verifies the redownloaded files"
	@echo "  4. Removes successful files from tracker"
	@echo ""
	uv run bhds aws-download ${CONFIG_FILE}

clear-failed:
	@echo "🗑️  Clearing failed files tracker..."
	@rm -f ~/crypto_data/bhds/aws_data/.failed_files.json
	@echo "✅ Failed files tracker cleared"

format:
	@echo "🎨 Formatting code..."
	uv run black .
	uv run isort .

test:
	@echo "🧪 Running tests..."
	uv run tests/aws_client.py
	uv run tests/aws_downloader.py
	uv run tests/test_checksum_validation.py

clean:
	@echo "🧹 Cleaning cache files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	@echo "✅ Clean complete"

# Example workflows for different configs
download-spot-1h:
	@$(MAKE) download-verify CONFIG_FILE=configs/download/spot_1h.yaml

download-spot-1d:
	@$(MAKE) download-verify CONFIG_FILE=configs/download/spot_1d.yaml

download-futures:
	@$(MAKE) download-verify CONFIG_FILE=configs/download/futures_kline.yaml

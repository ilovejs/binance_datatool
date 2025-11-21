.PHONY: help setup download verify clean format test

# Configuration
CONFIG_FILE ?= configs/download/spot_1h.yaml

help:
	@echo "BHDS - Binance Historical Data Service"
	@echo ""
	@echo "Available targets:"
	@echo "  make setup              - Install dependencies and activate virtualenv"
	@echo "  make download           - Download data (behavior controlled by config)"
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
	@echo "  2. make download           # Download data based on config"
	@echo ""
	@echo "How it works:"
	@echo "  - The behavior (verification, retrying) is controlled by the config file."
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
	@echo "📥 Downloading/Verifying data..."
	@echo "Config: ${CONFIG_FILE}"
	@echo "Note: Verification and retry behavior is defined in the config file."
	uv run bhds aws-download ${CONFIG_FILE}

list-failed:
	@cat ~/crypto_data/bhds/aws_data/.failed_files.json

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
	@$(MAKE) download CONFIG_FILE=configs/download/spot_1h.yaml

download-spot-1d:
	@$(MAKE) download CONFIG_FILE=configs/download/spot_1d.yaml

download-futures:
	@$(MAKE) download CONFIG_FILE=configs/download/futures_kline.yaml

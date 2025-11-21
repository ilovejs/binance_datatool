#!/usr/bin/env python3
"""
BHDS AWS Download Task.

This module implements the download workflow to fetch historical market data from Binance's official AWS data center.
It loads a YAML config, resolves symbols, downloads missing files, and optionally verifies checksums.
"""
import os
from itertools import chain
from pathlib import Path

from bdt_common.constants import BINANCE_AWS_DATA_PREFIX, HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import divider, logger
from bdt_common.network import create_aiohttp_session
from bhds.aws.checksum import (ChecksumVerifier,
                               validate_and_cleanup_invalid_checksums)
from bhds.aws.client import AwsClient, create_aws_client_from_config
from bhds.aws.downloader import AwsDownloader
from bhds.aws.failed_files import FailedFilesTracker
from bhds.aws.local import LocalAwsClient
from bhds.tasks.common import (create_symbol_filter_from_config, get_bhds_home,
                               load_config)


class AwsDownloadTask:
    """
    Coordinates downloading historical Binance data from the official AWS data center for the public bhds application.

    The task loads config, filters target symbols, downloads missing archives, and optionally verifies checksums.
    """

    def __init__(self, config_path: str | Path):
        """Initialize the download task from a YAML configuration.

        Args:
            config_path: Path to the YAML config file.

        Raises:
            KeyError: If required fields (trade_type, data_type, data_freq) are missing.
        """
        self.config = load_config(config_path)
        logger.info(f"Loaded configuration from: {config_path}")

        # Get top-level params
        bhds_home = get_bhds_home(self.config.get("bhds_home"))
        self.aws_data_dir = bhds_home / "aws_data"

        # Initialize failed files tracker
        self.failed_files_tracker = FailedFilesTracker(
            bhds_home / "aws_data" / ".failed_files.json"
        )
        self.http_proxy = (
            self.config.get("http_proxy")
            or os.getenv("HTTP_PROXY")
            or os.getenv("http_proxy")
        )
        self.use_proxy_for_aria2c = self.config.get("use_proxy_for_aria2c", False)
        logger.info(f"📁 BHDS home: {bhds_home}")
        logger.info(f"📁 Download directory: {self.aws_data_dir}")
        logger.info(
            f"🌐 HTTP proxy: {self.http_proxy or 'None'}, "
            f"Use proxy for aria2c: {self.use_proxy_for_aria2c}"
        )

        if "trade_type" not in self.config:
            raise KeyError("Missing 'trade_type' in config")
        self.trade_type = TradeType(
            self.config["trade_type"]
        )  # e.g. "spot", "futures/um", "futures/cm"

        if "data_type" not in self.config:
            raise KeyError("Missing 'data_type' in config")
        self.data_type = DataType(self.config["data_type"])  # e.g. "klines"

        if "data_freq" not in self.config:
            raise KeyError("Missing 'data_freq' in config")
        self.data_freq = DataFrequency(
            self.config["data_freq"]
        )  # e.g. "daily", "monthly"

        self.verification_config: dict = self.config.get("checksum_verification")

    def _apply_symbol_filter(self, all_symbols: list[str]) -> list[str]:
        """Apply the configured symbol filter to the full symbol list.

        Args:
            all_symbols: All available symbols returned by the AWS client.

        Returns:
            The filtered list of symbols. If no filter is configured, returns the input list unchanged.
        """
        filter_cfg = self.config.get("symbol_filter")

        if filter_cfg is None or not filter_cfg:
            logger.info("No symbol filtering applied, using all symbols")
            return all_symbols

        symbol_filter = create_symbol_filter_from_config(self.trade_type, filter_cfg)
        filtered_symbols = symbol_filter(all_symbols)
        return filtered_symbols

    def _get_target_symbols(self, all_symbols: list[str]) -> list[str]:
        """Determine the final set of symbols to process.

        If config['symbols'] is provided and non-empty, intersect it with the available symbols.
        Otherwise, apply the configured symbol filter.

        Args:
            all_symbols: All available symbols returned by the AWS client.

        Returns:
            A sorted list of target symbols to be processed.
        """
        symbols = self.config.get("symbols")
        if symbols:
            valid = sorted(set(symbols).intersection(set(all_symbols)))
            logger.info(f"Using {len(valid)} user-specified symbols")
            return valid

        # Fallback to configured symbol filter
        filtered = self._apply_symbol_filter(all_symbols)
        logger.info(
            f"Found {len(all_symbols)} total symbols, Filtered to {len(filtered)} symbols"
        )
        return filtered

    async def _download_files(self, client: AwsClient, symbols: list[str]) -> None:
        """Download data files for the given symbols.

        Lists files via the AWS client, then downloads only the missing files into aws_data_dir.
        Optionally routes aria2c traffic through an HTTP proxy when enabled.

        Args:
            client: AWS data client used to list and build file paths.
            symbols: Symbols to download.
        """
        logger.info(f"📊 Processing {len(symbols)} symbols...")

        logger.info(f"🔍 Fetching file lists for {len(symbols)} symbols from AWS...")
        files_map = await client.batch_list_data_files(symbols)

        # Log summary
        total_files = sum(len(files) for files in files_map.values())
        if total_files == 0:
            logger.warning("No files found")
            return

        logger.info(f"📊 Total: {total_files} files across {len(symbols)} symbols")

        # Only pass http_proxy to AwsDownloader if use_proxy_for_aria2c is True
        proxy_for_downloader = self.http_proxy if self.use_proxy_for_aria2c else None
        downloader = AwsDownloader(
            local_dir=self.aws_data_dir, http_proxy=proxy_for_downloader
        )

        # Download per symbol and report progress
        for idx, (symbol, files) in enumerate(files_map.items(), 1):
            if not files:
                continue
            downloader.aws_download(files)
            logger.ok(f"✅ [{idx}/{len(symbols)}] {symbol} saved ({len(files)} files)")

    def _verify_files(self, client: AwsClient) -> None:
        """Verify checksums for downloaded files and optionally delete mismatches.

        Args:
            client: AWS data client (used for path building when scanning local files).
        """
        divider("BHDS: Verifying Downloaded Files", sep="-")

        verifier = ChecksumVerifier(
            delete_mismatch=self.verification_config.get("delete_mismatch", False)
        )

        # Use LocalAwsClient to get all unverified files
        local_client = LocalAwsClient(self.aws_data_dir, client.path_builder)
        all_symbols_status = local_client.get_all_symbols_status()

        all_unverified_files = []
        for symbol_status in all_symbols_status.values():
            all_unverified_files.extend(symbol_status["unverified"])

        if all_unverified_files:
            # Fast pre-validation: Find and cleanup invalid checksum files BEFORE full verification
            # This avoids wasting time on expensive SHA256 verification when checksums are empty/corrupt
            logger.info(
                f"⚡ Pre-validating {len(all_unverified_files)} checksum files..."
            )
            invalid_checksum_files = validate_and_cleanup_invalid_checksums(
                all_unverified_files
            )

            # Track invalid checksum files for redownload
            if invalid_checksum_files:
                for data_file in invalid_checksum_files:
                    relative_path = data_file.relative_to(self.aws_data_dir)
                    data_url = f"{BINANCE_AWS_DATA_PREFIX}/{relative_path}"
                    checksum_url = f"{data_url}.CHECKSUM"

                    self.failed_files_tracker.add_failed_file(
                        data_file=data_file,
                        error="Invalid/empty checksum file",
                        url=data_url,
                        checksum_url=checksum_url,
                    )

                # Remove invalid files from verification list
                all_unverified_files = [
                    f for f in all_unverified_files if f not in invalid_checksum_files
                ]

                logger.warning(
                    f"⚠️ Skipped {len(invalid_checksum_files)} files with invalid checksums (tracked for redownload)"
                )

        if all_unverified_files:
            logger.debug(f"🔍 Verifying {len(all_unverified_files)} files")
            results = verifier.verify_files(all_unverified_files)

            # Clear successfully verified files from tracker
            if results["success"] > 0:
                successful_files = [
                    f for f in all_unverified_files if f not in results["errors"]
                ]
                self.failed_files_tracker.clear_successful_files(successful_files)

            # Log results
            if results["failed"] > 0:
                logger.warning(
                    f"⚠️ Verification complete: {results['success']} success, {results['failed']} failed"
                )
                logger.warning(f"❌ Failed files:")
                for file_path, error in results["errors"].items():
                    logger.warning(f"  - {file_path}: {error}")

                # Separate checksum mismatches from checksum file issues
                checksum_mismatches = {
                    path: error
                    for path, error in results["errors"].items()
                    if "Checksum mismatch" in error
                }
                checksum_file_issues = {
                    path: error
                    for path, error in results["errors"].items()
                    if "Checksum mismatch" not in error
                }

                # Log checksum file issues (these should have been caught in pre-validation!)
                if checksum_file_issues:
                    logger.error(
                        f"⚠️ BUG: {len(checksum_file_issues)} files with checksum file issues "
                        f"reached verifier (should be caught in pre-validation!)"
                    )
                    for path, error in checksum_file_issues.items():
                        logger.error(f"  - {path}: {error}")

                # Only cleanup files with actual checksum mismatches
                if checksum_mismatches:
                    if not self.verification_config.get("delete_mismatch", False):
                        logger.warning(
                            "⚠️ Invalid files found but delete_mismatch=false in config"
                        )
                        logger.warning(
                            "💡 Set delete_mismatch=true to automatically remove invalid files"
                        )
                    else:
                        logger.warning(
                            f"🗑️  Deleted {len(checksum_mismatches)} files with checksum mismatches"
                        )
                        # Only cleanup files with actual data corruption
                        self._cleanup_invalid_files(checksum_mismatches)
            else:
                logger.ok(
                    f"✅ Verification complete: {results['success']} success, {results['failed']} failed"
                )
        else:
            logger.ok("✅ All files already verified")

    def _cleanup_invalid_files(self, failed_files: dict) -> None:
        """Remove invalid files and track them for retry.

        This ensures invalid files are deleted immediately and tracked for redownload.

        Args:
            failed_files: Dictionary mapping file paths to error messages.
        """
        from pathlib import Path

        from bhds.aws.checksum import get_checksum_file, get_verified_file

        for file_path, error in failed_files.items():
            data_file = Path(file_path)

            # Build URLs for redownload
            # Convert local path to AWS path
            relative_path = data_file.relative_to(self.aws_data_dir)
            data_url = f"{BINANCE_AWS_DATA_PREFIX}/{relative_path}"
            checksum_url = f"{data_url}.CHECKSUM"

            # Track failed file for retry
            self.failed_files_tracker.add_failed_file(
                data_file=data_file,
                error=error,
                url=data_url,
                checksum_url=checksum_url,
            )

            # Delete data file
            if data_file.exists():
                data_file.unlink()
                logger.debug(f"🗑️  Removed invalid data file: {data_file.name}")

            # Delete verified marker
            verified_file = get_verified_file(data_file)
            if verified_file.exists():
                verified_file.unlink()

            # Delete checksum file
            checksum_file = get_checksum_file(data_file)
            if checksum_file.exists():
                checksum_file.unlink()

    def _retry_failed_files(self) -> None:
        """Retry downloading previously failed files using aria2c directly."""
        if not self.failed_files_tracker.has_failed_files():
            return

        failed_count = self.failed_files_tracker.get_count()
        logger.info(f"🔄 Found {failed_count} previously failed files, retrying...")

        # Get URLs to retry (includes both data and checksum files)
        retry_urls = self.failed_files_tracker.get_retry_urls()

        if not retry_urls:
            return

        logger.info(f"📥 Redownloading {len(retry_urls)} files (data + checksums)...")

        # Use downloader to retry with aria2c
        proxy_for_downloader = self.http_proxy if self.use_proxy_for_aria2c else None
        downloader = AwsDownloader(
            local_dir=self.aws_data_dir, http_proxy=proxy_for_downloader
        )

        # Convert URLs back to PurePosixPath format for downloader
        from pathlib import PurePosixPath

        aws_files = []
        for url in retry_urls:
            # Remove prefix to get relative path
            if url.startswith(BINANCE_AWS_DATA_PREFIX + "/"):
                relative_path = url[len(BINANCE_AWS_DATA_PREFIX) + 1 :]
                aws_files.append(PurePosixPath(relative_path))

        if aws_files:
            downloader.aws_download(aws_files)
            logger.ok(f"✅ Retry download completed for {len(aws_files)} files")

    async def run(self):
        """Execute the end-to-end download workflow.

        Steps:
            1. Check for previously failed files and retry them first.
            2. Auto-detect if only retrying (skip symbol listing for speed).
            3. Otherwise, create HTTP session and list symbols.
            4. Download missing files.
            5. Optionally verify checksums.
            6. Track any new failures for next run.
        """
        divider("BHDS: Start Binance AWS Download", with_timestamp=True)

        # Check if we have failed files to retry
        has_failed_files = self.failed_files_tracker.has_failed_files()

        if has_failed_files:
            # Retry previously failed files first
            self._retry_failed_files()

            # Smart optimization: If user explicitly set retry_only=true in config, skip symbol listing
            if self.config.get("retry_only", False):
                logger.info(
                    "⚡ Fast retry mode: Skipping symbol listing (use retry_only=false to disable)"
                )
                async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
                    client = create_aws_client_from_config(
                        self.trade_type,
                        self.data_type,
                        self.data_freq,
                        self.config.get("time_interval"),
                        session,
                        self.http_proxy,
                    )
                    if self.verification_config:
                        self._verify_files(client)

                # Report final status
                if self.failed_files_tracker.has_failed_files():
                    logger.warning(
                        f"⚠️ {self.failed_files_tracker.get_count()} files still failing after retry"
                    )
                    logger.warning("💡 Run the command again to retry failed files")
                else:
                    logger.ok("✅ All files downloaded and verified successfully")

                divider("BHDS: Binance AWS Download Completed", with_timestamp=True)
                return

        # Normal mode: List symbols and download
        async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
            client = create_aws_client_from_config(
                self.trade_type,
                self.data_type,
                self.data_freq,
                self.config.get("time_interval"),
                session,
                self.http_proxy,
            )

            logger.debug("🔍 Fetching available symbols...")
            all_symbols = await client.list_symbols()

            target_symbols = self._get_target_symbols(all_symbols)

            if not target_symbols:
                logger.warning("⚠️ No symbols to process after filtering")
                return

            await self._download_files(client, target_symbols)
            if self.verification_config:
                self._verify_files(client)

        # Report final status
        if self.failed_files_tracker.has_failed_files():
            logger.warning(
                f"⚠️ {self.failed_files_tracker.get_count()} files still failing after retry"
            )
            logger.warning("💡 Run the command again to retry failed files")
        else:
            logger.ok("✅ All files downloaded and verified successfully")

        divider("BHDS: Binance AWS Download Completed", with_timestamp=True)

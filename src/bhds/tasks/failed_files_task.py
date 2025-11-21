"""
Task for managing failed files.
"""
import os
from pathlib import Path

from bdt_common.log_kit import logger
from bhds.aws.downloader import AwsDownloader
from bhds.aws.failed_files import FailedFilesTracker
from bhds.tasks.common import get_bhds_home


class FailedFilesTask:
    """
    Task for managing failed files (listing, retrying, clearing).
    """

    def __init__(self):
        """Initialize the failed files task."""
        bhds_home = get_bhds_home()
        self.aws_data_dir = bhds_home / "aws_data"
        self.tracker = FailedFilesTracker(self.aws_data_dir / ".failed_files.json")
        
        # Get proxy settings from env
        self.http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")

    def list_failed(self) -> None:
        """List all failed files."""
        failed_files = self.tracker.get_failed_files()
        if not failed_files:
            logger.ok("✅ No failed files found.")
            return

        logger.warning(f"⚠️ Found {len(failed_files)} failed files:")
        for path, info in failed_files.items():
            logger.warning(f"  - {path}")
            logger.warning(f"    Error: {info['error']}")
            logger.warning(f"    Attempts: {info.get('attempts', 0)}")
            logger.warning("")

    def retry(self) -> None:
        """Retry downloading failed files."""
        if not self.tracker.has_failed_files():
            logger.ok("✅ No failed files to retry.")
            return

        retry_urls = self.tracker.get_retry_urls()
        logger.info(f"🔄 Retrying {len(retry_urls)} files (data + checksums)...")

        downloader = AwsDownloader(
            local_dir=self.aws_data_dir,
            http_proxy=self.http_proxy
        )

        from pathlib import PurePosixPath
        from bdt_common.constants import BINANCE_AWS_DATA_PREFIX

        aws_files = []
        for url in retry_urls:
            if url.startswith(BINANCE_AWS_DATA_PREFIX + "/"):
                relative_path = url[len(BINANCE_AWS_DATA_PREFIX) + 1 :]
                aws_files.append(PurePosixPath(relative_path))

        if aws_files:
            downloader.aws_download(aws_files)
            logger.ok(f"✅ Retry download completed for {len(aws_files)} files")
            logger.info("💡 Run 'make download' or 'bhds aws-download' to verify the downloaded files.")

    def clear(self) -> None:
        """Clear the failed files tracker."""
        self.tracker.clear_all()
        logger.ok("✅ Failed files tracker cleared.")

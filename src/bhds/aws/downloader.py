import os
import shutil
import subprocess
import tempfile
from pathlib import Path, PurePosixPath
from typing import Optional

from bdt_common.constants import ARIA2C_QUIET, BINANCE_AWS_DATA_PREFIX
from bdt_common.log_kit import logger


def get_aria2c_exec() -> str:
    # Check if aria2c exists in the PATH
    aria2c_path = shutil.which("aria2c")
    if not aria2c_path:
        raise FileNotFoundError(
            f"aria2c executable not found in system PATH: {os.getenv('PATH')}"
        )
    return aria2c_path


def aria2_download_files(
    download_infos: list[tuple[str, Path]], http_proxy: Optional[str] = None
) -> int:
    """
    Download files from AWS S3 using aria2c command-line tool.

    Args:
        download_infos: List of tuples containing (aws_url, local_file_path) pairs
        http_proxy: HTTP proxy URL string, or None for no proxy

    Returns:
        int: Exit code from aria2c process (0 for success, non-zero for failure)
    """
    # Create temporary file containing download URLs and directory mappings for aria2c
    with tempfile.NamedTemporaryFile(
        mode="w", delete_on_close=False, prefix="bhds_"
    ) as aria_file:
        # Write each download URL and its target directory to the temp file
        # Also ensure parent directories exist
        for aws_url, local_file in download_infos:
            local_file.parent.mkdir(parents=True, exist_ok=True)
            aria_file.write(f"{aws_url}\n  dir={local_file.parent}\n")
        aria_file.close()

        # Build aria2c command with optimized settings for parallel downloads
        aria2c_path = get_aria2c_exec()
        cmd = [
            aria2c_path,
            "-i",
            aria_file.name,
            "-j32",  # max concurrent downloads
            "-x4",  # max connections per file
        ]

        # Add quiet flag if enabled to suppress verbose aria2c logs
        if ARIA2C_QUIET:
            cmd.append("-q")

        # Add proxy configuration if provided
        if http_proxy is not None:
            cmd.append(f"--https-proxy={http_proxy}")

        # Execute aria2c download process
        run_result = subprocess.run(cmd, env={})
        returncode = run_result.returncode
    return returncode


def find_missings(download_infos: list[tuple[str, Path]]) -> list[tuple[str, Path]]:
    """
    Identify missing files that need to be downloaded from AWS S3.

    Args:
        download_infos: List of tuples containing (aws_url, local_file_path) pairs

    Returns:
        list[tuple[str, Path]]: Filtered list containing only files that don't exist locally
    """
    return [(url, path) for url, path in download_infos if not path.exists()]


class AwsDownloader:
    """
    AWS S3 file downloader for Binance data with retry and batching capabilities.
    """

    def __init__(self, local_dir: Path, http_proxy: str = None):
        """
        Initialize the AWS downloader with configuration parameters.

        Args:
            local_dir: Local directory path where files will be downloaded
            http_proxy: HTTP proxy URL string for downloads, or None for direct connection
        """
        self.local_dir = local_dir
        self.http_proxy = http_proxy

    def aws_download(self, aws_files: list[PurePosixPath], max_tries=3):
        """
        Download multiple files from AWS S3 with retry logic and batch processing.

        Args:
            aws_files: List of PurePosixPath objects representing AWS S3 file paths
            max_tries: Maximum number of retry attempts for failed downloads (default: 3)
        """
        # Build list of download information (URL, local path) for all files
        download_infos = [
            (f"{BINANCE_AWS_DATA_PREFIX}/{str(aws_file)}", self.local_dir / aws_file)
            for aws_file in aws_files
        ]

        # Retry loop for handling failed downloads
        for try_id in range(max_tries):
            # Find which files are still missing (need to be downloaded)
            missing_infos = find_missings(download_infos)

            # Exit if all files have been successfully downloaded
            if not missing_infos:
                break

            # Process downloads in batches to avoid overwhelming the system
            batch_size = 4096
            for i in range(0, len(missing_infos), batch_size):
                batch_infos = missing_infos[i : i + batch_size]
                aria2_download_files(batch_infos, self.http_proxy)

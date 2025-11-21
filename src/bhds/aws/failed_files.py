"""Failed files tracker for managing download retry logic."""

import json
from pathlib import Path
from typing import Optional


class FailedFilesTracker:
    """Tracks failed file downloads and verifications for retry logic."""

    def __init__(self, tracking_file: Path):
        """Initialize the failed files tracker.

        Args:
            tracking_file: Path to JSON file storing failed file information.
        """
        self.tracking_file = tracking_file
        self.failed_files: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        """Load failed files from tracking file."""
        if self.tracking_file.exists():
            try:
                with open(self.tracking_file, "r") as f:
                    self.failed_files = json.load(f)
            except Exception:
                # If file is corrupted, start fresh
                self.failed_files = {}

    def _save(self) -> None:
        """Save failed files to tracking file."""
        self.tracking_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.tracking_file, "w") as f:
            json.dump(self.failed_files, f, indent=2)

    def add_failed_file(
        self,
        data_file: Path,
        error: str,
        url: str,
        checksum_url: Optional[str] = None,
    ) -> None:
        """Add a failed file to the tracker.

        Args:
            data_file: Path to the failed data file.
            error: Error message describing the failure.
            url: Download URL for the data file.
            checksum_url: Optional download URL for the checksum file.
        """
        file_key = str(data_file)
        self.failed_files[file_key] = {
            "data_file": str(data_file),
            "error": error,
            "url": url,
            "checksum_url": checksum_url,
            "attempts": self.failed_files.get(file_key, {}).get("attempts", 0) + 1,
        }
        self._save()

    def add_failed_files_batch(self, failed_files: dict[Path, dict]) -> None:
        """Add multiple failed files at once.

        Args:
            failed_files: Dictionary mapping file paths to their failure info
                         (must contain 'error', 'url', and optionally 'checksum_url').
        """
        for data_file, info in failed_files.items():
            self.add_failed_file(
                data_file=data_file,
                error=info["error"],
                url=info["url"],
                checksum_url=info.get("checksum_url"),
            )

    def get_failed_files(self) -> dict[str, dict]:
        """Get all tracked failed files.

        Returns:
            Dictionary mapping file paths to their failure information.
        """
        return self.failed_files.copy()

    def get_retry_urls(self) -> list[str]:
        """Get list of URLs to retry downloading.

        Returns both data file URLs and checksum URLs that need to be redownloaded.

        Returns:
            List of URLs to download.
        """
        urls = []
        for file_info in self.failed_files.values():
            # Always redownload the data file
            urls.append(file_info["url"])

            # Also redownload checksum if it exists
            if file_info.get("checksum_url"):
                urls.append(file_info["checksum_url"])

        return urls

    def remove_file(self, data_file: Path) -> None:
        """Remove a file from the failed files tracker.

        Args:
            data_file: Path to the data file to remove.
        """
        file_key = str(data_file)
        if file_key in self.failed_files:
            del self.failed_files[file_key]
            self._save()

    def clear_successful_files(self, successful_files: list[Path]) -> None:
        """Remove successfully verified files from the tracker.

        Args:
            successful_files: List of file paths that passed verification.
        """
        for file_path in successful_files:
            self.remove_file(file_path)

    def clear_all(self) -> None:
        """Clear all failed files from tracker."""
        self.failed_files = {}
        self._save()

    def get_count(self) -> int:
        """Get the number of tracked failed files.

        Returns:
            Number of failed files.
        """
        return len(self.failed_files)

    def has_failed_files(self) -> bool:
        """Check if there are any failed files tracked.

        Returns:
            True if there are failed files, False otherwise.
        """
        return len(self.failed_files) > 0

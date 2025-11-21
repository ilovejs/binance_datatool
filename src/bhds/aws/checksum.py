import hashlib
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from bdt_common.log_kit import logger


def get_checksum_file(data_file: Path) -> Path:
    """
    Get the path to the checksum file corresponding to a data file.

    Args:
        data_file: Path to the data file

    Returns:
        Path to the .CHECKSUM file in the same directory as the data file
    """
    checksum_file = data_file.parent / (data_file.name + ".CHECKSUM")
    return checksum_file


def get_verified_file(data_file: Path) -> Path:
    """
    Get the path to the verification mark file corresponding to a data file.

    Args:
        data_file: Path to the data file

    Returns:
        Path to the .verified file in the same directory as the data file
    """
    verified_file = data_file.parent / (data_file.name + ".verified")
    return verified_file


def calc_checksum(data_file: Path) -> str:
    """
    Calculate SHA256 checksum of the file by reading the file content and computing its SHA256 hash.

    Args:
        data_file: Path to the file to calculate checksum for

    Returns:
        SHA256 checksum as a hexadecimal string
    """
    with open(data_file, "rb") as file_to_check:
        data = file_to_check.read()
        checksum_value = hashlib.sha256(data).hexdigest()
    return checksum_value


def validate_and_cleanup_invalid_checksums(data_files: list[Path]) -> list[Path]:
    """
    Fast pre-validation: Find and remove invalid checksum files (missing, empty, or corrupted).

    This avoids wasting time verifying data files when their checksums are already broken.
    Invalid checksum files are deleted so they can be redownloaded.

    Args:
        data_files: List of data files to check

    Returns:
        List of data files whose checksum files are invalid (need redownload)
    """
    invalid_files = []

    for data_file in data_files:
        checksum_file = get_checksum_file(data_file)

        # Check if checksum file is missing
        if not checksum_file.exists():
            logger.warning(
                f"🗑️  Missing checksum file: {checksum_file.name}, marking for checksum redownload..."
            )
            # Do NOT delete data file, only redownload checksum
            # Ensure verified marker is gone so it gets re-verified
            get_verified_file(data_file).unlink(missing_ok=True)
            invalid_files.append(data_file)
            continue

        # Check if checksum file is empty or too small (< 32 bytes for SHA256)
        try:
            file_size = checksum_file.stat().st_size
            if file_size == 0:
                logger.warning(
                    f"🗑️  Empty checksum file: {checksum_file.name}, deleting..."
                )
                checksum_file.unlink()
                # Do NOT delete data file, only redownload checksum
                get_verified_file(data_file).unlink(missing_ok=True)
                invalid_files.append(data_file)
                continue

            # Quick validation: try to read the checksum
            with open(checksum_file, "r") as f:
                content = f.read().strip()

            if not content or len(content.split()) == 0:
                logger.warning(
                    f"🗑️  Invalid checksum file: {checksum_file.name}, deleting..."
                )
                checksum_file.unlink()
                # Do NOT delete data file
                get_verified_file(data_file).unlink(missing_ok=True)
                invalid_files.append(data_file)

        except Exception as e:
            logger.warning(
                f"🗑️  Corrupted checksum file: {checksum_file.name}, deleting... ({e})"
            )
            checksum_file.unlink(missing_ok=True)
            # Do NOT delete data file
            get_verified_file(data_file).unlink(missing_ok=True)
            invalid_files.append(data_file)

    if invalid_files:
        logger.warning(
            f"⚡ Pre-validation: Found {len(invalid_files)} invalid checksum files, deleted for redownload"
        )

    return invalid_files


def read_checksum(checksum_path: Path) -> str:
    """
    Read checksum value from checksum file

    Args:
        checksum_path: Path to the checksum file

    Returns:
        Checksum value
    """
    if not checksum_path.exists():
        raise FileNotFoundError(f"Checksum file {checksum_path} not exists")

    try:
        with open(checksum_path, "r") as fin:
            text = fin.read().strip()

        if not text:
            raise ValueError("Empty checksum file")

        # Checksum file format: "checksum_value filename" or just "checksum_value"
        parts = text.split()
        if len(parts) == 0:
            raise ValueError("Empty checksum file")
        elif len(parts) == 1:
            # Only checksum value, no filename
            checksum_standard = parts[0]
        else:
            # Standard format: checksum_value filename
            checksum_standard = parts[0]

        return checksum_standard
    except Exception as e:
        raise RuntimeError(f"Invalid checksum file {checksum_path.name}: {e}")


class ChecksumVerifier:
    """Checksum verifier for validating AWS data file integrity"""

    def __init__(self, delete_mismatch: bool = False, n_jobs: Optional[int] = None):
        """
        Initialize checksum verifier

        Args:
            delete_mismatch: Whether to delete file if verification fails
            n_jobs: Number of parallel processes, defaults to CPU cores - 2
        """
        self.delete_mismatch = delete_mismatch
        self.n_jobs = n_jobs or max(1, mp.cpu_count() - 2)

    def verify_file(self, data_file: Path) -> bool:
        """
        Verify checksum of a single file

        IMPORTANT: This should ONLY be called after pre-validation.
        Files with missing/empty checksums should be caught by validate_and_cleanup_invalid_checksums()
        BEFORE reaching this method.

        Args:
            data_file: Path to the data file to verify

        Returns:
            Success flag

        Raises:
            FileNotFoundError: If checksum file is missing (should be caught in pre-validation!)
            RuntimeError: If checksum file is invalid (should be caught in pre-validation!)
        """
        checksum_path = get_checksum_file(data_file)

        # Read checksum (will raise if missing/invalid - this is a bug if it happens!)
        try:
            checksum_standard = read_checksum(checksum_path)
        except (FileNotFoundError, RuntimeError) as e:
            # This should NEVER happen if pre-validation was run correctly
            logger.error(
                f"BUG: Checksum file issue reached verifier (should be caught in pre-validation): {e}"
            )
            # DO NOT delete the data file - it might be valid!
            # Re-raise so the error gets logged properly
            raise

        checksum_value = calc_checksum(data_file)

        if checksum_value != checksum_standard:
            # Only delete if actual SHA256 mismatch (data file is corrupted)
            if self.delete_mismatch:
                self._cleanup_files(data_file)
            return False

        # Create verification mark
        verified_file = get_verified_file(data_file)
        verified_file.touch()
        return True

    def verify_files(self, files: list[Path]) -> dict:
        """
        Batch verify files

        Args:
            files: List of files to verify

        Returns:
            Dictionary containing verification results
        """
        results = {"success": 0, "failed": 0, "errors": {}, "total_files": len(files)}

        if not files:
            return results

        with tqdm(total=len(files), desc="Verifying files", unit="file") as pbar:
            with ProcessPoolExecutor(max_workers=self.n_jobs) as executor:
                future_to_file = {
                    executor.submit(self.verify_file, f): f for f in files
                }

                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        success = future.result()
                        if success:
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["errors"][file_path] = "Checksum mismatch"
                    except Exception as e:
                        results["failed"] += 1
                        results["errors"][file_path] = str(e)

                    pbar.update(1)
                    pbar.set_postfix(
                        {"success": results["success"], "failed": results["failed"]}
                    )

        return results

    def _cleanup_files(self, data_file: Path) -> None:
        """
        Cleanup files after verification failure

        Args:
            data_file: Path to the data file that failed verification
        """
        from bdt_common.log_kit import logger

        data_file.unlink(missing_ok=True)

        verified_file = get_verified_file(data_file)
        verified_file.unlink(missing_ok=True)

        checksum_file = get_checksum_file(data_file)
        checksum_file.unlink(missing_ok=True)

        logger.debug(f"🗑️  Deleted invalid file: {data_file}")

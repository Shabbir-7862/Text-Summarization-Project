import os
import zipfile
import requests
from pathlib import Path
from TextSummarizer.logging import logger
from TextSummarizer.utils.common import get_size
from TextSummarizer.entity import (DataIngestionConfig)



class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config
        
    def download_file(self):
        """Download data.zip from source_URL using safe streaming."""
        url = self.config.source_URL
        out_path = Path(self.config.local_data_file)
        tmp_path = out_path.with_suffix(out_path.suffix + ".part")

        # if file exists, skip download
        if out_path.exists():
            logger.info(f"File already exists of size: {get_size(out_path)}")
            return

        logger.info(f"Downloading from: {url}")

        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()  # throws error for HTTP != 200

                out_path.parent.mkdir(parents=True, exist_ok=True)

                # write to .part temporary file first
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        if chunk:
                            f.write(chunk)

            # ensure non-zero file
            if tmp_path.stat().st_size == 0:
                raise RuntimeError("Downloaded file is empty (0 bytes).")

            # move .part → final filename
            tmp_path.replace(out_path)

            logger.info(
                f"{out_path} downloaded successfully! "
                f"Size: {get_size(out_path)}"
            )

        except Exception as e:
            logger.exception(f"Download failed: {e}")
            if tmp_path.exists():
                tmp_path.unlink()  # remove partial file
            raise e

    def extract_zip_file(self):
        """Safely extract ZIP file member-by-member with clear error logging."""
        zip_path = Path(self.config.local_data_file)
        extract_dir = Path(self.config.unzip_dir)

        if not zip_path.exists():
            raise FileNotFoundError(f"{zip_path} does not exist")

        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                members = z.namelist()

                for i, member in enumerate(members, start=1):
                    target = extract_dir / member

                    # Create directories as needed
                    if member.endswith("/") or member.endswith("\\"):
                        target.mkdir(parents=True, exist_ok=True)
                        continue

                    target.parent.mkdir(parents=True, exist_ok=True)

                    # Extract file in chunks
                    with z.open(member) as src, open(target, "wb") as dst:
                        while True:
                            chunk = src.read(1024 * 64)  # 64 KB
                            if not chunk:
                                break
                            dst.write(chunk)
                        dst.flush()
                        os.fsync(dst.fileno())

        except Exception as e:
            logger.exception(f"Error while extracting ZIP: {e}")
            raise

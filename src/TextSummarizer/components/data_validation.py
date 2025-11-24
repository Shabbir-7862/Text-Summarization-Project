import os
from pathlib import Path
from TextSummarizer.logging import logger
from TextSummarizer.entity import DataValidationConfig



class DataValidation:

    def __init__(self, config: DataValidationConfig):
        self.config = config


    def validate_all_files_exist(self) -> bool:
        try:
            all_files = os.listdir(os.path.join("artifacts", "data_ingestion", "extracted_data", "samsum_dataset"))

            # ensure the status file's parent directory exists
            status_path = Path(self.config.STATUS_FILE)
            status_path.parent.mkdir(parents=True, exist_ok=True)

            # Check if ALL required files exist
            validation_status = True
            for required_file in self.config.ALL_REQUIRED_FILES:
                if required_file not in all_files:
                    validation_status = False
                    break

            # Write status once, at the end
            with open(status_path, "w") as f:
                f.write(f"Validation Status: {validation_status}\n")

            return validation_status
        
        except Exception as e:
            raise e


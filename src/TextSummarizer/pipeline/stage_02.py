from TextSummarizer.logging import logger
from TextSummarizer.config.configuration import configurationManager
from TextSummarizer.components.data_validation import DataValidation


class DataValidationTrainingPipeline:
    def __init__(self) -> None:
        pass

    def main(self):
        config = configurationManager()
        data_validation_config = config.get_data_validation_config()
        data_validation = DataValidation(config=data_validation_config)
        data_validation.validate_all_files_exist()
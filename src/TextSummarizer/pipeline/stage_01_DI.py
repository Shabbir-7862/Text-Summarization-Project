from TextSummarizer.logging import logger
from TextSummarizer.config.configuration import configurationManager
from TextSummarizer.components.data_ingestion import DataIngestion


class DataIngestionTrainingPipeline:
    def __init__(self) -> None:
        pass

    def main(self):
        config = configurationManager()
        data_ingestion_config = config.get_data_ingestion_config()
        data_ingestion = DataIngestion(config=data_ingestion_config)
        data_ingestion.download_file()
        data_ingestion.extract_zip_file()
from TextSummarizer.logging import logger
from TextSummarizer.config.configuration import configurationManager
from TextSummarizer.components.data_transformation import DataTransformation


class DataTransformationTrainingPipeline:
    def __init__(self) -> None:
        pass

    def main(self):
        config = configurationManager()
        data_transformation_config = config.get_data_transformation_config()
        data_transformation = DataTransformation(config=data_transformation_config)
        data_transformation.convert()
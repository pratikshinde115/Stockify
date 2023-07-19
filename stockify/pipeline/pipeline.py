from stockify.exception import StockifyExpection
from stockify.logger import logging
from stockify.config.configuration import Configuartion
import os, sys
from stockify.entity.artifact_entity import DataIngestionArtifact
from stockify.entity.config_entity import  DataIngestionConfig
from stockify.components.data_ingestion import DataIngestion

class Pipepline:
    def __init__(self,config:Configuartion = Configuartion()):
        try:
            self.config=config
        except Exception as e:
            raise StockifyExpection(e,sys) from e
        
    def start_data_ingestion(self) -> DataIngestionArtifact:
            try:
                data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
                return data_ingestion.initiate_data_ingestion()
            except Exception as e:
                raise StockifyExpection(e, sys) from e
  


    def run_pipeline(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()
        except Exception as e:
            raise StockifyExpection(e, sys) from e

from cgi import test
from sklearn import preprocessing
from stockify.exception import StockifyExpection
from stockify.logger import logging
from stockify.entity.config_entity import DataTransformationConfig 
from stockify.entity.artifact_entity import DataIngestionArtifact,\
DataValidationArtifact,DataTransformationArtifact
import sys,os

from sklearn.preprocessing import MinMaxScaler

import pandas as pd
from stockify.constant import *
from stockify.util.util import save_numpy_array_data






class DataTransformation:

    def __init__(self, data_transformation_config: DataTransformationConfig,
                 data_ingestion_artifact: DataIngestionArtifact,
                 data_validation_artifact: DataValidationArtifact
                 ):
        try:
            logging.info(f"{'>>' * 30}Data Transformation log started.{'<<' * 30} ")
            self.data_transformation_config= data_transformation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_artifact = data_validation_artifact

        except Exception as e:
            raise StockifyExpection(e,sys) from e
        


    def get_data_transformer(self)->DataTransformationConfig:
        try:
  

            logging.info(f"Obtaining training and test file path.")


            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            
            
            train_df = pd.read_csv(train_file_path)
            
            test_df = pd.read_csv(test_file_path)



            min_max = MinMaxScaler(feature_range=(0,1))

            train_df =  min_max.fit_transform(train_df)
            test_df =  min_max.transform(test_df)



            transformed_train_dir = self.data_transformation_config.transformed_train_dir
            transformed_test_dir = self.data_transformation_config.transformed_test_dir

            train_file_name = os.path.basename(train_file_path)
            test_file_name = os.path.basename(test_file_path)

            transformed_train_file_path = os.path.join(transformed_train_dir, train_file_name)
            transformed_test_file_path = os.path.join(transformed_test_dir, test_file_name)

            logging.info(f"Saving transformed training and testing.")
            # train_df.to_csv(transformed_train_file_path,index=False)
            # test_df.to_csv(transformed_test_file_path,index=False)
            
            save_numpy_array_data(file_path=transformed_train_file_path,array=train_df)
            save_numpy_array_data(file_path=transformed_test_file_path,array=test_df)



            data_transformation_artifact = DataTransformationArtifact(
            is_transformed=True,
            message="Data transformation successfull.",
            transformed_train_file_path=transformed_train_file_path,
            transformed_test_file_path=transformed_test_file_path,
            

            )

            logging.info(f"Data transformationa artifact: {data_transformation_artifact}")

        except Exception as e:
            raise StockifyExpection(e,sys) from e
        return data_transformation_artifact
    


    def __del__(self):  
        logging.info(f"{'>>'*30}Data Transformation log completed.{'<<'*30} \n\n")
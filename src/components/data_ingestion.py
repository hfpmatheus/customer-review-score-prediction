import logging
import pandas as pd
import subprocess
from dataclasses import dataclass
from zenml import step
import os

@dataclass
class DataIngestionConfig:
    dataset_username: str = 'olistbr'
    dataset_name: str = 'brazilian-ecommerce'
    dataset_path: str = 'olist_order_reviews_dataset.csv'
    destination_directory: str = 'artifacts/data/raw/'

class DataIngestion:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
    
    def ingest_data(self):
        try:
            logging.info("Initiated data ingestion.")
            os.makedirs(self.data_ingestion_config.destination_directory, exist_ok=True)

            extract_command = f"kaggle datasets download -d {self.data_ingestion_config.dataset_username}/{self.data_ingestion_config.dataset_name} \
                                -f {self.data_ingestion_config.dataset_path} -p {self.data_ingestion_config.destination_directory}"
            subprocess.run(extract_command, shell="True")
            logging.info("Extracted .zip dataset from kaggle.")

            unzip_command = f"unzip -o {self.data_ingestion_config.destination_directory}/{self.data_ingestion_config.dataset_path}.zip \
                              -d {self.data_ingestion_config.destination_directory}"
            subprocess.run(unzip_command, shell="True")
            logging.info("Unzipped the dataset.")

            os.remove(f"{self.data_ingestion_config.destination_directory}/{self.data_ingestion_config.dataset_path}.zip")
            logging.info("Deleted the .zip dataset and finished data ingestion.")

        except Exception as e:
            logging.error(f"Error during data ingestion: {str(e)}")

if __name__ == "__main__":
    data_ingestion = DataIngestion()
    data_ingestion.ingest_data()
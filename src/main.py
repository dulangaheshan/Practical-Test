# Author: Dulanga Heshan
import os
import logging
from utils import create_spark_session, read_csv_file
from tasks import task1, task2, task3
import configparser

env = os.getenv('ENVIRONMENT', default='local')
config = configparser.ConfigParser()
config.read('config.ini')
conf = config[env]

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, filename=conf.get("log_file_path"))

def main():
    logging.info('Start The Spark Session')
    spark = create_spark_session("Practical_Test", conf)
    input_file_path = os.path.join("data", "source", "Online Retail.xlsx")
    csv_converted_file_path = os.path.join("data", "csv_converted", "Online Retail.csv")
    partitioned_file_path = os.path.join("data", "partitioned", "Online Retail.csv")
    task1_file_path = os.path.join("data", "output", "avg_sales.csv")
    task2_file_path = os.path.join("data", "output", "top_10_on_dec_UK_.csv")

    logging.info('Processing Task 1')
    task1(spark, input_file_path, csv_converted_file_path, partitioned_file_path)

    logging.info('read partitioned data set and cache')
    partitioned_df = read_csv_file(spark, partitioned_file_path)
    partitioned_df.cache()

    logging.info('Processing Task 2')
    task2(partitioned_df, task1_file_path)

    logging.info('Processing Task 3')
    task3(partitioned_df, task2_file_path)

    spark.stop()
    logging.info('Finished The Spark Session')


if __name__ == "__main__":
    main()

# Author: Dulanga Heshan

import os
from pyspark.sql.functions import col, avg
import logging
from utils import create_spark_session, read_csv_file, convert_excel_to_csv, write_partitioned_csv, write_csv_file
from tasks import task1, task2, task3

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, filename='logs'
                                                                                                   '/practical_test.log')


def main():
    logging.info('Start The Spark Session')
    spark = create_spark_session("Practical_Test")

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

# Author: Dulanga Heshan

from pandas import DataFrame
import pandas as pd
import logging
from pyspark.sql import SparkSession



def create_spark_session(app_name: str, conf):
    """
    create a spark session

    :param app_name: application name for the spark app
    :return: spark session
    """
    try:
        return SparkSession.builder.appName(conf.get('app_name')).getOrCreate()
    except Exception as e:
        logging.error("Error while creating SparkSession:", str(e))


def convert_excel_to_csv(excel_file_path, csv_file_path):
    """
    Convert an Excel file to a CSV file.

    :param excel_file_path: File path of the Excel file to be converted.
    :param csv_file_path: File path of the CSV file to be written.
    """
    logging.info('convert excel file to csv:-' + excel_file_path)
    try:
        df = pd.read_excel(excel_file_path)
        df.to_csv(csv_file_path, index=False)
    except Exception as e:
        logging.error("Error while Convert excel to csv:", str(e))


def read_csv_file(spark, file_path: str, header: bool = True, infer_schema: bool = True):
    """

    read a csv file

    :param spark: spark session
    :param file_path: file path of the csv file
    :param header: default true
    :param infer_schema:
    :return: df object for the csv
    """
    logging.info('reading csv:-' + file_path)
    try:
        return spark.read.csv(file_path, header=header, inferSchema=infer_schema)
    except Exception as e:
        logging.error("Error while reading CSV File:", str(e))


def write_csv_file(df, file_path: str):
    """
     Writes a dataframe to a CSV file.

    :param df: dataframe
    :param file_path: write path of the location
    :return: void
    """
    logging.info('Writing csv:-' + file_path)
    try:
        df.write.mode("overwrite").option("header", "true").option("ignoreSuccessFile", "true") \
            .option("checksum", "false").csv(file_path)
    except Exception as e:
        logging.error("Error while writing CSV file:", str(e))


def write_partitioned_csv(dataframe: DataFrame, partition_col: str, output_path: str, mode: str = "overwrite"):
    """
    Write a PySpark DataFrame to partitioned CSV files.
    :param dataframe: the DataFrame to write
    :param partition_col: the name of the column to partition the data by
    :param output_path: the path to write the partitioned CSV files
    :param mode: the write mode (default is "overwrite")
    """
    logging.info('Writing partitioned csv:-' + output_path)
    try:
        dataframe.write.mode(mode).option("header", "true").partitionBy(partition_col).csv(output_path)
    except Exception as e:
        logging.error("Error while writing partition csv files:", str(e))

from pandas import DataFrame
from pyspark.sql import SparkSession
import pandas as pd


def create_spark_session(app_name: str):
    """
    create a spark session

    :param app_name: application name for the spark app
    :return: spark session
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def convert_excel_to_csv(excel_file_path, csv_file_path):
    """
    Convert an Excel file to a CSV file.

    :param excel_file_path: File path of the Excel file to be converted.
    :param csv_file_path: File path of the CSV file to be written.
    """
    print(excel_file_path)
    df = pd.read_excel(excel_file_path)
    df.to_csv(csv_file_path, index=False)


def read_csv_file(spark, file_path: str, header: bool = True, infer_schema: bool = True):
    """

    read a csv file

    :param spark: spark session
    :param file_path: file path of the csv file
    :param header: default true
    :param infer_schema:
    :return: df object for the csv
    """

    print(file_path)

    return spark.read.csv(file_path, header=header, inferSchema=infer_schema)


def write_csv_file(df, file_path: str):
    """
     Writes a dataframe to a CSV file.

    :param df: dataframe
    :param file_path: write path of the location
    :return: void
    """
    df.write.mode("overwrite").option("header", "true").csv(file_path)


def write_partitioned_csv(dataframe: DataFrame, partition_col: str, output_path: str, mode: str = "overwrite"):
    """
    Write a PySpark DataFrame to partitioned CSV files.
    :param dataframe: the DataFrame to write
    :param partition_col: the name of the column to partition the data by
    :param output_path: the path to write the partitioned CSV files
    :param mode: the write mode (default is "overwrite")
    """
    dataframe.write.mode(mode).partitionBy(partition_col).csv(output_path)

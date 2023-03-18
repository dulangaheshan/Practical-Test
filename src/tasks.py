# Author: Dulanga Heshan

import logging
import os
from utils import create_spark_session, read_csv_file, convert_excel_to_csv, write_partitioned_csv, write_csv_file
from pyspark.sql.functions import col, avg


def task1(spark, input_file_path: str, csv_converted_file_path: str, partitioned_file_path: str):
    """
    Read data and converted to partitions and save

    :param spark: spark session
    :param input_file_path: excel file location
    :param csv_converted_file_path: excel converted to csv file save location
    :param partitioned_file_path: partition result save location
    :return: void
    """
    logging.info('Start task1 read file and converted to csv partitions')
    try:
        logging.info('Processing Task 1 The Spark Session')
        convert_excel_to_csv(input_file_path, csv_converted_file_path)
        input_df = read_csv_file(spark, csv_converted_file_path)
        write_partitioned_csv(input_df, "Country", partitioned_file_path, mode="overwrite")
    except Exception as e:
        logging.warning('Error while processing Task 1', str(e))


def task2(df, task1_file_path: str):
    """
    Calculate average subtotal (sales value) per country.

    :param df: dataframe
    :param task1_file_path: output result write location
    :return:
    """
    logging.info('Start task2 - Calculate average subtotal (sales value) per country')
    try:

        df = df.repartition("Country")
        task1_df = df.withColumn("sales_value", col("Quantity") * col("UnitPrice")) \
            .groupBy("Country") \
            .agg(avg("sales_value").alias("average_sales_value"))
        task1_df = task1_df.repartition(1)
        write_csv_file(task1_df, task1_file_path)
    except Exception as e:
        logging.warning('Error while processing Task 2', str(e))


def task3(df, task2_file_path):
    """
    sales records where country is United Kingdom. and Top 10 products by sales value during the month of December 2010

    :param df: dataframe
    :param task2_file_path: output result write location
    :return:
    """
    logging.info('Start task3 - sales records where country is United Kingdom')
    try:
        uk_filtered_df = df.filter(col("Country") == "United Kingdom")
        date_filtered_df = uk_filtered_df.filter(col("InvoiceDate").between("2010-11-30", "2011-01-31"))
        task2_df = date_filtered_df.withColumn("total_sales_value", col("Quantity") * col("UnitPrice")) \
            .orderBy(col("total_sales_value").desc()).limit(10).select("InvoiceNo", "Description", "total_sales_value")
        task2_df = task2_df.repartition(1)
        write_csv_file(task2_df, task2_file_path)
    except Exception as e:
        logging.warning('Error while processing Task 3', str(e))

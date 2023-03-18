import os
from pyspark.sql.functions import col

from utils import create_spark_session, read_csv_file, convert_excel_to_csv, write_partitioned_csv


def main():
    spark = create_spark_session("My PySpark App")

    input_file_path = os.path.join("data", "source", "Online Retail.xlsx")
    csv_converted_file_path = os.path.join("data", "csv_converted", "Online Retail.csv")
    partitioned_file_path = os.path.join("data", "partitioned", "Online Retail.csv")

    convert_excel_to_csv(input_file_path, csv_converted_file_path)
    input_df = read_csv_file(spark, csv_converted_file_path)
    write_partitioned_csv(input_df, "Country", partitioned_file_path, mode="overwrite")

    # input_df.show()
    spark.stop()


if __name__ == "__main__":
    main()
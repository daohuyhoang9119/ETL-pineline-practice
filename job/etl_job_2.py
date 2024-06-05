## Install libraries
from datetime import date, datetime
import os
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import timedelta
 


spark = SparkSession.builder.config("spark.driver.memory", "10g").getOrCreate()

PSQL_SERVERNAME = "localhost"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "PostgreSQL 16"
PSQL_USERNAME = "postgres"
PSQL_PASSWORD = "admin123"


def read_data(path):
    try:
        df = spark.read.json(path)
        return df
    except Exception as e:
        print(f"Error reading data from {path}")
        return None



def transform_data(df):
    df = df.select('_source.AppName', '_source.Contract', '_source.Mac', '_source.TotalDuration')
    return df

def handle_category(df, date):
    data = df.withColumn('Type',
        when((col('AppName') == 'CHANNEL') | (col('AppName') =='DSHD')| (col('AppName') =='KPLUS')| (col('AppName') =='KPlus'), 'Truyền Hình')
        .when((col('AppName') == 'VOD') | (col('AppName') =='FIMS_RES')| (col('AppName') =='BHD_RES')| 
                (col('AppName') =='VOD_RES')| (col('AppName') =='FIMS')| (col('AppName') =='BHD')| (col('AppName') =='DANET'), 'Phim Truyện')
        .when((col('AppName') == 'RELAX'), 'Giải Trí')
        .when((col('AppName') == 'CHILD'), 'Thiếu Nhi')
        .when((col('AppName') == 'SPORT'), 'Thể Thao')
        .otherwise('Error'))
    data = data.withColumn("Date", lit(date)) 
    return data


def pivot_data(df):
    data = df.groupBy("Date","Contract","Type").agg((sum('TotalDuration').alias("TotalDuration")))
    data = data.groupBy('Date','Contract').pivot('Type').sum('TotalDuration')
    data = data.fillna(0)
    return data 

input_path = "D:\\Đại Học CNTT\\Data engineer\\DE-COURSE\\Homework\\ETL Pineline\\data\\"
output_path = "D:\\Đại Học CNTT\\Data engineer\\DE-COURSE\\Homework\\ETL Pineline\\output\\final-data"


def get_date(filename):
    date = filename.split(".")[0]
    date = datetime.strptime(date, "%Y%m%d").date()
    return date

def save_as_csv(df, output):
    try:
        df.repartition(1).write.csv(output, header=True, mode="overwrite")
        print("CSV file written successfully.")
    except Exception as e:
        print(f"Error writing CSV file: {e}")

    return None

def save_to_DB(df):
    print("------Start importing data to Pgadmin Database-----")
    df = df.write.format("jdbc").option("url", "jdbc:postgresql:/localhost:5432/PostgreSQL 16").option("dbtable", 'user_log').option("user",  PSQL_USERNAME).option("password", PSQL_PASSWORD).save()
    print("------Done import to Database-----")
    return None


def main(path):
    print("---------Reading files from folder--------------")
    files = os.listdir(path)
    print(files)
    start_date = datetime.strptime("20220401", "%Y%m%d").date()
    end_date = datetime.strptime("20220404", "%Y%m%d").date()

    # Create an empty DataFrame with the same schema as the target DataFrame
    schema = StructType([
        StructField("AppName", StringType(), True),
        StructField("Contract", StringType(), True),
        StructField("Mac", StringType(), True),
        StructField("Type", StringType(), True),
        StructField("TotalDuration", LongType(), True),
        StructField("Date", DateType(), True)
    ])
    final_df = spark.createDataFrame([], schema=schema)

    for filename in files:
        if filename.endswith('.json'):
            date = get_date(filename)
            print(date)
            # Check if file is in the date range
            if (date >= start_date) & (date <= end_date):
                print(f"Reading file {filename}...")
                df = read_data(input_path+filename)
                df = transform_data(df)
                df = handle_category(df, date)
                final_df = final_df.unionByName(df)

    final_df.show(5,truncate=False)
    print("--------- Pivot the data--------------")
    final_df = pivot_data(final_df)
    final_df.show(5,truncate=False)
     # Check if output path exists
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    #save as csv
    save_as_csv(final_df, output_path)

    # save_to_DB(final_df)
    return print("Task finished") 

main(input_path)


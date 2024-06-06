## Install libraries
from datetime import date, datetime
import os
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv, dotenv_values

input_path = "D:\\Đại Học CNTT\\Data engineer\\DE-COURSE\\Homework\\ETL Pineline\\data\\"
output_path = "D:\\Đại Học CNTT\\Data engineer\\DE-COURSE\\Homework\\ETL Pineline\\output\\final-data"

spark = SparkSession.builder.config("spark.driver.memory", "10g").getOrCreate()

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
    # load_dotenv()
    # os.getenv("PSQL_PORTNUMBER")

    print("------Start importing data to Pgadmin Database-----")
    df.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/ETL Bigdata").option("dbtable", 'user_log').option("user", "postgres").option("password", "admin123").save()
    print("------Done import to Database-----")
    return None


def create_most_watch_column(df):
    columns_to_compare = ["Giải Trí", "Phim Truyện", "Thiếu Nhi", "Thể Thao", "Truyền Hình"]
    max_col_expr = coalesce(*[when(col(c) == greatest(*columns_to_compare), lit(c)).otherwise(None) for c in columns_to_compare])
    df = df.withColumn("most_watch", max_col_expr)
    return df

def create_customer_taste(df):
    data = df.withColumn('Customer_Taste',
        concat_ws("-",
            when(
                (col('Giải Trí') != 0), "Giải Trí"),
            when(
                (col('Phim Truyện') != 0), "Phim Truyện"),
            when(
                (col('Thiếu Nhi') != 0),"CT Thiếu Nhi"),
            when(
                (col('Thể Thao') != 0),"Thể Thao"),
            when(
                (col('Truyền Hình') != 0),"Truyền Hình")
            )
        )
    return data



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
    # save_as_csv(final_df, output_path)

    #save to Postgres
    # save_to_DB(final_df)


    test1 = create_most_watch_column(final_df)
    test1.show(5,truncate=False)
    
    test2 = create_customer_taste(test1)
    test2.show(5,truncate=False)
    
    return print("Task finished") 

main(input_path)


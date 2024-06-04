## Install libraries
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
 

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()


def read_data(path):
    try:
        df = spark.read.json(path)
        return df
    except Exception as e:
        print(f"Error reading data from {path}")
        return None



def transform_data(df):
    df = df.select('_source.AppName', '_source.Contract', '_source.Mac', '_source.TotalDuration')
    df = df.withColumn("Date", current_date()) 
    return df

def handle_category(df):
    data = df.withColumn('Type',
        when((col('AppName') == 'CHANNEL') | (col('AppName') =='DSHD')| (col('AppName') =='KPLUS')| (col('AppName') =='KPlus'), 'Truyền Hình')
        .when((col('AppName') == 'VOD') | (col('AppName') =='FIMS_RES')| (col('AppName') =='BHD_RES')| 
                (col('AppName') =='VOD_RES')| (col('AppName') =='FIMS')| (col('AppName') =='BHD')| (col('AppName') =='DANET'), 'Phim Truyện')
        .when((col('AppName') == 'RELAX'), 'Giải Trí')
        .when((col('AppName') == 'CHILD'), 'Thiếu Nhi')
        .when((col('AppName') == 'SPORT'), 'Thể Thao')
        .otherwise('Error'))
    return data


def pivot_data(df):
    data = df.groupBy("Date","Contract","Type").agg((sum('TotalDuration').alias("TotalDuration")))
    data = data.groupBy('Date','Contract').pivot('Type').sum('TotalDuration')
    data = data.fillna(0)
    return data 

input_path = "D:\\Đại Học CNTT\\Data engineer\\DE-COURSE\\Homework\\ETL Pineline\\data\\20220401.json"
out_path = "D:\\Đại Học CNTT\\Data engineer\\DE-COURSE\\Homework\\ETL Pineline\\output\\log1"

def main(path):
    print("---------Reading data from source--------------")
    df = read_data(path)
    df.show(5,truncate=False)
    print("---------Transforming data --------------")
    df = transform_data(df)
    print("---------Handle category--------------")
    df = handle_category(df)
    df.show(5,truncate=False)
    print("---------Pivot--------------")
    df = pivot_data(df)
    print("---------Printing output--------------")
    df.show(5,truncate=False)
    print("---------Saving output--------------")
    df.repartition(1).write.csv('D:\\Đại Học CNTT\\Data engineer\\DE-COURSE\\Homework\\ETL Pineline\\output\\log1-test').header("true")
    # df.repartition(1).write.option("header", "true").csv(out_path)
    return print("Task finished") 

main(input_path)


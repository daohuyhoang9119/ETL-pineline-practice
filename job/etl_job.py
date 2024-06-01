## Install libraries
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()


def read_data(path):
    df = spark.read.json(path)
    return df 

def print_data(df):
    return df.show(10)

def load_data():
    return None



pathData = "D:\\Đại Học CNTT\\Data engineer\\DE-COURSE\\Homework\\ETL Pineline\\data\\20220401.json"

def main(path):
    print("---------Reading data from source--------------")
    df = read_data(path)
    df.show(10,truncate=False)
    df.select('_source').show(10,truncte=False)
    return None

main(pathData)


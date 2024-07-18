import config as conf
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(conf.ProjectName).getOrCreate()
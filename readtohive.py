from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS nazir_db")

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432"
                                             "/testdb") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "nazir_data") \
    .option("user", "consultants").option("password", "WelcomeItc@2022").load()
#df.printSchema()



df.write.mode("overwrite").saveAsTable("nazir_db.nazir_hive")
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS nazir_db")

# df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432"
#                                              "/testdb") \
#     .option("driver", "org.postgresql.Driver").option("dbtable", "nazir_data") \
#     .option("user", "consultants").option("password", "WelcomeItc@2022").load()
#df.printSchema()



# PostgresSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}
postgres_table_name = "nazir_fload"

# hive database and table names
# hive_database_name = "nazir_db"
# hive_table_name = "carinsuranceclaims"


# 2. Load new data dataset from PostgresSQL:
df = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)




df.write.mode("overwrite").saveAsTable("nazir_db.nazir_hive")
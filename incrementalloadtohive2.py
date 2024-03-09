from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Incrementalload2").enableHiveSupport().getOrCreate()

# Reading existing hive table
existing_hive_data = spark.sql("SELECT * from nazir_db.nazir_hive")

# PostgresSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}

# Postgress table with updated info
postgres_table_name = "nazir_data"

# Load new data dataset from PostgresSQL:
new_data = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)


# Max time from existing hive table
max_existing_time = existing_hive_data.selectExpr("max(Time) as max_time").collect()[0]["max_time"]


# Filter new data based on the maximum existing Time
incremental_data = new_data.filter(col("Time") > max_existing_time)

# Append incremental data to existing data
updated_data = existing_hive_data.union(incremental_data)

# Write updated data to Hive table
updated_data.write.mode("overwrite").saveAsTable("nazir_db.nazir_hive")

# Stop SparkSession
spark.stop()

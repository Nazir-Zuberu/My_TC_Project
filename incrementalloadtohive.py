from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Incrementalload").enableHiveSupport().getOrCreate()

# Reading existing hive table
existing_hive_data = spark.sql("SELECT * from nazir_db.nazir_hive")

# PostgresSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}
postgres_table_name = "nazir_iload"

# 2. Load new data dataset from PostgresSQL:
new_data = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)


# Max time from existing hive table
max_existing_time = existing_hive_data.selectExpr("max(Time) as max_time").collect()[0]["max_time"]


# Filter new data based on the maximum existing id
incremental_data = new_data.filter(col("Time") > max_existing_time)

# Append incremental data to existing data
updated_data = existing_hive_data.union(incremental_data)

# Write updated data to Hive table
updated_data.write.mode("overwrite").saveAsTable("nazir_db.nazir_hive")

# Stop SparkSession
spark.stop()




















########################################################
# from os.path import abspath
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# # Create spark session with hive enabled
# spark = SparkSession.builder.appName("carInsuranceClaimsApp").enableHiveSupport().getOrCreate()
# # .config("spark.jars", "/Users/hmakhlouf/Desktop/TechCnsltng_WorkSpace/config/postgresql-42.7.2.jar") \


# # 1- Establish the connection to PostgresSQL and Redshift:
# # PostgresSQL connection properties
# postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
# postgres_properties = {
#     "user": "consultants",
#     "password": "WelcomeItc@2022",
#     "driver": "org.postgresql.Driver",
# }
# postgres_table_name = "car_insurance_claims"

# # hive database and table names
# hive_database_name = "project1db"
# hive_table_name = "carinsuranceclaims"


# # 2. Load new data dataset from PostgresSQL:
# new_data = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
# new_data.show(3)

# # 3. Load the existing_data in hive table
# existing_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))

# existing_hive_data = spark.read.table("project1db.carinsuranceclaims")

# existing_hive_data.show(3)


# # 4. Determine the incremental data
# incremental_data = new_data.filter(~col("id").isin(existing_hive_data.select("id")))


# #incremental_data = new_data.filter(~new_data.ID.isin(existing_hive_data.select("ID")))
# # 5.  Adding the new records to the existing hive table
# new_hive_table = existing_hive_data.union(incremental_data)


# # 6 . Writing the updated DataFrame back to the Hive table
# new_hive_table.write.mode("overwrite").saveAsTable("your_existing_table")


# # Stop Spark session
# spark.stop()

##########################################################


# from pyspark.sql import *
# from pyspark.sql.functions import *

# spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()
# max_id = spark.sql("SELECT max(id) FROM product.emp_info")
# m_id = max_id.collect()[0][0]
# str(m_id)

# query = 'SELECT * FROM emp_info WHERE "ID" > ' + str(m_id)

# more_data = spark.read.format("jdbc") \
#     .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
#     .option("driver", "org.postgresql.Driver") \
#     .option("user", "consultants") \
#     .option("password", "WelcomeItc@2022") \
#     .option("query", query) \
#     .load()

# df_age = more_data.withColumn("DOB", to_date(col("DOB"), "M/d/yyyy")) \
#     .withColumn("age", floor(datediff(current_date(), col("DOB")) / 365))
# df_age.show(10)

# # Define the increments based on departments and gender
# department_increment_expr = when(col("dept") == "IT", 0.1) \
#     .when(col("dept") == "Marketing", 0.12) \
#     .when(col("dept") == "Purchasing", 0.15) \
#     .when(col("dept") == "Operations", 0.18) \
#     .when(col("dept") == "Finance", 0.2) \
#     .when(col("dept") == "Management", 0.25) \
#     .when(col("dept") == "Research and Development", 0.15) \
#     .when(col("dept") == "Sales", 0.18) \
#     .when(col("dept") == "Accounting", 0.15) \
#     .when(col("dept") == "Human Resources", 0.12) \
#     .otherwise(0)

# # Calculate the increment based on department and gender
# increment_expr = when(col("gender") == "Female", department_increment_expr + 0.1).otherwise(department_increment_expr)

# # Calculate the incremented salary based on department and gender
# df_increment = df_age.withColumn("increment", col("salary") * increment_expr) \
#     .withColumn("new_salary", col("salary") + col("increment"))

# # Show the updated DataFrame
# df_increment.show(10)

# # Sort the DataFrame by ID
# sorted_df = df_increment.orderBy("ID")
# sorted_df.show(10)

# df_increment.write.mode("append").saveAsTable("product.emp_info")
# print("Successfully Load to Hive")




# spark-submit --master local[*] --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar src/IncreamentalLoadPostgressToHive.py

# df2 = spark.read.csv("path/to/other_file.csv", header=True, inferSchema=True)
# joined_df = df.join(df2, on=["ID"], how="inner")

# df1.write.mode("overwrite").saveAsTable("product.dummy")
# hadoop fs -chmod -R 775 /warehouse/tablespace/external/hive/product.db/emp_info
# sudo -u hdfs hdfs dfs -chmod -R 777 /warehouse/tablespace/external/hive/product.db

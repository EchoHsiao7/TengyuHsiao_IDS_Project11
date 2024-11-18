from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CSV Statistics and SQL").getOrCreate()

# Read CSV file into a DataFrame
file_path = "Steam_2024_bestRevenue_1500.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Describe the statistics of the DataFrame
df_description = df.describe()

# Perform an easy SQL query: create a temporary view and select top 5 rows
df.createOrReplaceTempView("steam_data")
sql_query_result = spark.sql("SELECT * FROM steam_data LIMIT 5")

# Display outputs
df_description.show()
sql_query_result.show()

# Stop Spark session
spark.stop()

from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, when, sum, to_date, trim
from pyspark.sql.window import Window
from google.colab import drive

drive.mount('/content/drive')

# Initializing spark session
spark = SparkSession.builder.appName("CurrentBalanceCalculation").getOrCreate()

# Reading the input data file
df_trans = pd.read_excel('/content/drive/MyDrive/Colab Notebooks/Worksheet in Problem Definition.xlsm')
df = spark.createDataFrame(df_trans)

# Selecting only relevant columns and dropping NaN rows
df = df.select("TransactionDate", "AccountNumber", "TransactionType", "Amount").dropna()

# Type Casting columns in required format &
# Trimming the TransactionType column as there are leading/trailing spaces in the data
df = df.withColumn("TransactionDate", col("TransactionDate").cast(IntegerType())) \
       .withColumn("AccountNumber", col("AccountNumber").cast(IntegerType())) \
       .withColumn("TransactionType", trim(col("TransactionType"))) \
       .withColumn("Amount", col("Amount").cast(IntegerType()))

# Convert TransactionDate to date format as per expected output (yyyy-mm-dd)
df = df.withColumn("TransactionDate", to_date("TransactionDate",'yyyyMMdd'))

# Defining a window partition based on account number
window = Window.partitionBy("AccountNumber").orderBy("TransactionDate")  \
               .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate the sum of amounts after each transaction to get current balance based on the window
df = df.withColumn("CurrentBalance", sum(when(col("TransactionType") == "Credit", col("Amount")).otherwise(-col("Amount"))).over(window))

# Displaying the result
df.show()
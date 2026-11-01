from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# Initialize SparkSession
spark = SparkSession.builder.appName("MultipleDelimiterExample").getOrCreate()

# Create a sample DataFrame
data = [("apple,banana;orange|grape",),
        ("one two:three four",)]
df = spark.createDataFrame(data, ["text_column"])

df.show()

# Define the regex pattern for multiple delimiters (comma, semicolon, pipe, space, colon)
# The '|' acts as an OR operator in regex
delimiter_pattern = "[,;|\\s:]"

# Split the 'text_column' using the defined pattern
df_split = df.withColumn("split_values", split(col("text_column"), delimiter_pattern))

df_split.show(truncate=False)

# Access individual elements from the split array (optional)
df_elements = df_split.withColumn("first_element", col("split_values").getItem(0)) \
                      .withColumn("second_element", col("split_values").getItem(1))

df_elements.show(truncate=False)

# Stop SparkSession
spark.stop()
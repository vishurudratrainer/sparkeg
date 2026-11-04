"""Advanced PySpark Performance Tuning Masterclass - All-in-One Runner Script"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import broadcast


def run_masterclass():
  # Initialize Optimized SparkSession
  spark = SparkSession.builder.appName("Advanced_PySpark_Masterclass") \
      .master("local[*]") \
      .config("spark.sql.adaptive.enabled", "true") \
      .config("spark.sql.adaptive.skewJoin.enabled", "true") \
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
      .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  print("=" * 60)
  print(" ADVANCED PYSPARK MASTERCLASS: EXECUTION SUITE")
  print("=" * 60)

  # Module 1: Catalyst Optimizer & Physical Plans
  print("\n--- [Module 1] Catalyst Optimizer & PushedFilters ---")
  df = spark.range(0, 10000).withColumn("val", F.col("id") * 2)
  filtered = df.filter("val > 5000")
  filtered.explain(True)

  # Module 3 & 10: Adaptive Query Execution & Skew Handling
  print("\n--- [Module 3 & 10] AQE & Skew Handling ---")
  print("AQE Enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
  print("Skew Join Enabled:", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))

  df1 = spark.range(0, 5000).withColumn("key", F.col("id") % 10)
  df2 = spark.range(0, 5000).withColumn("key", F.col("id") % 10)
  joined = df1.join(df2, "key")
  print("Joined Row Count:", joined.count())

  # Module 4 & 15: Broadcast Hash Joins
  print("\n--- [Module 4 & 15] Broadcast Hash Join ---")
  large_fact = spark.range(0, 10000).withColumn("user_id", F.col("id") % 100)
  small_dim = (
      spark.range(0, 5)
      .withColumn("user_id", F.col("id"))
      .withColumn("tier", F.lit("VIP"))
  )
  broadcast_joined = large_fact.join(broadcast(small_dim), "user_id")
  print("Broadcast Join Row Count:", broadcast_joined.count())

  # Module 31: Multi-Column Clustering (Z-Ordering Simulation)
  print("\n--- [Module 31] Multi-Column Clustering Layout ---")
  raw_df = spark.range(0, 2000).withColumn(
      "region", F.concat(F.lit("region_"), F.col("id") % 4)
  )
  clustered_df = raw_df.sort("region", "id")
  print("Clustered DataFrame Rows Prepared:", clustered_df.count())

  print("\n" + "=" * 60)
  print(" ALL MODULES EXECUTED SUCCESSFULLY!")
  print("=" * 60)
  spark.stop()


if __name__ == "__main__":
  run_masterclass()
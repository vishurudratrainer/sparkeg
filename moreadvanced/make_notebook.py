import json

# Define Jupyter Notebook JSON structure
notebook_data = {
    "cells": [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# 🚀 Advanced PySpark Performance Tuning Masterclass\n",
                "Welcome to the hands-on lab notebook. Run the cells sequentially below to explore Catalyst optimization, Adaptive Query Execution (AQE), Broadcast Joins, and partitioning strategies.",
            ],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Cell 1: Initialize Optimized SparkSession\n",
                "from pyspark.sql import SparkSession\n",
                "import pyspark.sql.functions as F\n",
                "\n",
                "spark = SparkSession.builder \\\n",
                "    .appName(\"Advanced_PySpark_Masterclass\") \\\n",
                "    .master(\"local[*]\") \\\n",
                "    .config(\"spark.sql.adaptive.enabled\", \"true\") \\\n",
                "    .config(\"spark.sql.adaptive.skewJoin.enabled\", \"true\") \\\n",
                "    .config(\"spark.serializer\", \"org.apache.spark.serializer.KryoSerializer\") \\\n",
                "    .getOrCreate()\n",
                "\n",
                "spark.sparkContext.setLogLevel(\"WARN\")\n",
                "print(\"SparkSession initialized successfully!\")\n",
                "print(\"Spark Version:\", spark.version)",
            ],
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "--- \n",
                "## Module 1: Catalyst Optimizer & Physical Plans\n",
                "Spark transforms high-level DataFrame code into logical plans, optimizes them via the Catalyst Optimizer, and compiles them into physical execution plans. Always inspect plans using `.explain(True)` to verify **PushedFilters**.",
            ],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Cell 2: Inspecting Physical Plans & PushedFilters\n",
                "df = spark.range(0, 10000).withColumn(\n",
                "    \"val\", F.col(\"id\") * 2\n",
                ")\n",
                "filtered = df.filter(\"val > 5000\")\n",
                "\n",
                "print('--- Physical Execution Plan ---')\n",
                "filtered.explain(True)",
            ],
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "--- \n",
                "## Module 3 & 10: Adaptive Query Execution (AQE) & Skew Handling\n",
                "AQE automatically detects uneven partition key distributions at runtime and dynamically splits skewed tasks to prevent executor bottlenecks."
            ],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Cell 3: Verifying AQE Flags and Testing Skew Join\n",
                "print(\n",
                "    \"AQE Enabled:\",\n",
                "    spark.conf.get(\"spark.sql.adaptive.enabled\"),\n",
                ")\n",
                "print(\n",
                "    \"Skew Join Enabled:\",\n",
                "    spark.conf.get(\"spark.sql.adaptive.skewJoin.enabled\"),\n",
                ")\n",
                "\n",
                "df1 = spark.range(0, 10000).withColumn(\n",
                "    \"key\", F.col(\"id\") % 10\n",
                ")\n",
                "df2 = spark.range(0, 10000).withColumn(\n",
                "    \"key\", F.col(\"id\") % 10\n",
                ")\n",
                "\n",
                "joined = df1.join(df2, \"key\")\n",
                "print(\"Joined Row Count:\", joined.count())",
            ],
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "--- \n",
                "## Module 4 & 15: Broadcast Hash Joins (BHJ)\n",
                "Broadcast Joins eliminate network shuffles by copying small dimension tables to all workers."
            ],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Cell 4: Executing a Broadcast Join\n",
                "from pyspark.sql.functions import broadcast\n",
                "\n",
                "large_fact = spark.range(0, 50000).withColumn(\n",
                "    \"user_id\", F.col(\"id\") % 1000\n",
                ")\n",
                "small_dim = spark.range(0, 10).withColumn(\n",
                "    \"user_id\", F.col(\"id\")\n",
                ").withColumn(\"tier\", F.lit(\"Premium\"))\n",
                "\n",
                "broadcast_joined = large_fact.join(broadcast(small_dim), \"user_id\")\n",
                "print(\"Broadcast Join Successful. Row Count:\", broadcast_joined.count())",
            ],
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "--- \n",
                "## Module 31: Multi-Column Clustering (Z-Ordering Simulation)\n",
                "Co-locating related data across multiple columns to maximize data skipping during file scans."
            ],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Cell 5: Simulating Multi-Column Sorting Layout\n",
                "raw_df = spark.range(0, 5000).withColumn(\n",
                "    \"region\", F.concat(F.lit(\"region_\"), F.col(\"id\") % 4)\n",
                ")\n",
                "\n",
                "clustered_df = raw_df.sort(\"region\", \"id\")\n",
                "print(\n",
                "    \"Clustered DataFrame ready. Total Rows:\",\n",
                "    clustered_df.count(),\n",
                ")\n",
                "\n",
                "# Clean teardown\n",
                "spark.stop()\n",
                "print(\n",
                "    \"SparkSession stopped cleanly. Lab notebook execution complete!\"\n",
                ")",
            ],
        },
    ],
    "metadata": {
        "language_info": {"name": "python"},
        "orig_nbformat": 4,
    },
    "nbformat": 4,
    "nbformat_minor": 2,
}

# Write notebook file
notebook_filename = "advanced_pyspark_masterclass.ipynb"
with open(notebook_filename, "w", encoding="utf-8") as f:
  json.dump(notebook_data, f, indent=2)

print(f"Jupyter Notebook successfully created and saved as '{notebook_filename}'!")
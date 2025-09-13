
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .appName("MyGoitSparkSandbox-Part1") \
    .getOrCreate()

nuek_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("nuek-vuh3.csv")

nuek_repart = nuek_df.repartition(2)

nuek_processed = nuek_repart \
    .where("final_priority < 3") \
    .select("unit_id", "final_priority") \
    .groupBy("unit_id") \
    .count()

# Extra filter
nuek_processed = nuek_processed.where("count > 2")

# Action
nuek_processed.collect()

input("Press Enter to continue...")

spark.stop()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .appName("MyGoitSparkSandbox-Part2") \
    .getOrCreate()

nuek_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("nuek-vuh3.csv")

nuek_repart = nuek_df.repartition(2)

nuek_processed = nuek_repart \
    .where("final_priority < 3") \
    .select("unit_id", "final_priority") \
    .groupBy("unit_id") \
    .count()

# Intermediate action
nuek_processed.collect()

# Extra filter
nuek_processed = nuek_processed.where("count > 2")

# Action
nuek_processed.collect()

input("Press Enter to continue...")

spark.stop()


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .appName("MyGoitSparkSandbox-Part3") \
    .getOrCreate()

nuek_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("nuek-vuh3.csv")

nuek_repart = nuek_df.repartition(2)

nuek_processed_cached = nuek_repart \
    .where("final_priority < 3") \
    .select("unit_id", "final_priority") \
    .groupBy("unit_id") \
    .count() \
    .cache()

# First action 
nuek_processed_cached.collect()

# Extra filter
nuek_processed = nuek_processed_cached.where("count > 2")

# Second action
nuek_processed.collect()

input("Press Enter to continue...")

# Free memory
nuek_processed_cached.unpersist()

spark.stop()

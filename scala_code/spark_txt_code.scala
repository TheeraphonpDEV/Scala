import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// COMMAND ----------

val conf = new SparkConf().setAppName("Scala Spark")

// COMMAND ----------

val sc = SparkContext.getOrCreate(conf)

// COMMAND ----------

val data = sc.textFile("/Desktop/sample_text_data-1.txt")

// COMMAND ----------

data.collect()
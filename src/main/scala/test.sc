
import org.apache.spark.sql.SparkSession

val spark= SparkSession
  .builder
  .appName("RDDApp")
  .master("local[4]")
  .getOrCreate()

println(spark)
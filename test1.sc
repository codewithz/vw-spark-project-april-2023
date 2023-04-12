import org.apache.spark.sql.SparkSession

val spark= SparkSession
  .builder
  .appName("RDDApp")
  .master("local[4]")
  .getOrCreate()

println(spark)

val lines=spark.sparkContext.parallelize(List(1,2,3,4,5))

println(lines.count())

print("Hello")

lines.count
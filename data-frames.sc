import org.apache.spark.sql._

val spark= SparkSession
  .builder

  .appName("TaxiOperationsDataFrameApp")
  .master("local[*]")

  .getOrCreate()

val sc=spark.sparkContext

spark
// Create RDD

val data: List[List[Any]] = List(
  List(1, "Neha", 10000), List(2, "Steve", 20000),
  List(3, "Kari", 30000),
  List(4, "Ivan", 40000),
  List(5, "Mohit", 50000)
)

val employeesRdd= sc.parallelize(data)

employeesRdd.collect()

import spark.implicits._
val employeeDF=employeesRdd.toDF


employeeDF.show()

//Create DataFrame and show content


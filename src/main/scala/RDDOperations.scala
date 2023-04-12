import org.apache.spark.sql._

case class Employee(id:Int,name:String,salary:Int)

object RDDOperations {

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder
      .appName("DataFrameApp")
      .master("local[*]")
      .getOrCreate()

    val sc=spark.sparkContext

    spark
    // Create RDD

    val data: List[Employee] = List(
      Employee(1, "Neha", 10000),
      Employee(2, "Steve", 20000),
      Employee(3, "Kari", 30000),
      Employee(4, "Ivan", 40000),
      Employee(5, "Mohit", 50000)
    )

    val employeesRdd= sc.parallelize(data)

    employeesRdd.collect()

    val employeeDF=spark.createDataFrame(employeesRdd)

    employeeDF.show()



    Thread.sleep(240000)
  }

  }

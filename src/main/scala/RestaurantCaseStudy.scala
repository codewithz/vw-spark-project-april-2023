import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object RestaurantCaseStudy {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder

      .appName("DataFramesOps")
      .master("local[*]")
      .getOrCreate()

//    Using sparkContext.setLogLevel() method you can
//      change the log level to the desired level.
//    Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    spark.sparkContext.setLogLevel("ERROR")

    // Reads a CSV file with header, called
    // Restaurants_in_Wake_County.csv,
    // stores it in a dataframe
    var wakeDF = spark.read.format("csv").option("header", "true")
      .load("data/eateries/Restaurants_in_Wake_County.csv")
    println("*** Right after ingestion")

    wakeDF.show(5)
    wakeDF.printSchema()
    println("We have " + wakeDF.count + " records.")

    // Reads a JSON file called Restaurants_in_Durham_County_NC.json, stores
    // it
    // in a dataframe
    var durhamDF = spark.
                    read.
                    format("json").
                    load("data/eateries/Restaurants_in_Durham_County_NC.json")
    println("*** Right after ingestion")
    durhamDF.show(5)
    durhamDF.printSchema()
    println("We have " + durhamDF.count + " records.")

//    #Both the datasets are in different structure.
//    #TO clean it
//    , we will need to add an identifier and then make column names similar
//
//    #1. In both the datasets we will add the column by name -county and it will hold the name of
//    #the county that restaurant belongs to

    wakeDF = wakeDF.withColumn("county", lit("Wake"))
    wakeDF.show(3,truncate=25,vertical=true)

    durhamDF=durhamDF.withColumn("county",lit("Durham"))
    durhamDF.show(3,truncate=25,vertical=true)

    // Let's transform our dataframe
    wakeDF = wakeDF
      .withColumnRenamed("HSISID", "datasetId")
      .withColumnRenamed("NAME", "name")
      .withColumnRenamed("ADDRESS1", "address1")
      .withColumnRenamed("ADDRESS2", "address2")
      .withColumnRenamed("CITY", "city")
      .withColumnRenamed("STATE", "state")
      .withColumnRenamed("POSTALCODE", "zip")
      .withColumnRenamed("PHONENUMBER", "tel")
      .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
      .withColumnRenamed("FACILITYTYPE", "type")
      .withColumnRenamed("X", "geoX")
      .withColumnRenamed("Y", "geoY")
      .drop("OBJECTID", "PERMITID", "GEOCODESTATUS")

    val drop_cols=List("fields", "geometry", "record_timestamp", "recordid")

    durhamDF = durhamDF
      .withColumn("datasetId", col("fields.id"))
      .withColumn("name", col("fields.premise_name"))
      .withColumn("address1", col("fields.premise_address1"))
      .withColumn("address2", col("fields.premise_address2"))
      .withColumn("city", col("fields.premise_city"))
      .withColumn("state", col("fields.premise_state"))
      .withColumn("zip", col("fields.premise_zip"))
      .withColumn("tel", col("fields.premise_phone"))
      .withColumn("dateStart", col("fields.opening_date"))
      .withColumn("type", split(col("fields.type_description"), " - ").getItem(1))
      .withColumn("geoX", col("fields.geolocation").getItem(0))
      .withColumn("geoY", col("fields.geolocation").getItem(1))
      .drop(drop_cols:_*)

//    # Assign an identifier for each record

    wakeDF=wakeDF.withColumn("id",
                            concat(col("state"),
                              lit("_"),
                              col("county"),
                              lit("_"),
                              col("datasetId")))

    durhamDF=durhamDF.withColumn("id",
                                  concat(col("state"),
                                    lit("_"),
                                    col("county"),
                                    lit("_"),
                                    col("datasetId")))


  combineDataframes(wakeDF,durhamDF)

    Thread.sleep(240000)
  }

  /**
   * Performs the union between the two dataframes.
   *
   * @param df1 Left Dataframe to union on
   * @param df2 Right Dataframe to union from
   */
  private def combineDataframes(df1: Dataset[Row], df2: Dataset[Row]): Unit = {
    val df = df1.unionByName(df2)
    df.show(5)
    df.printSchema()
    println("We have " + df.count + " records.")
    val partitionCount = df.rdd.getNumPartitions
    println("Partition count: " + partitionCount)
  }
}


import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SFFireCalls {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder

      .appName("DataFramesOps")
      .master("local[*]")
      .getOrCreate()


    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
    StructField("UnitID", StringType, true),
    StructField("IncidentNumber", IntegerType, true),
    StructField("CallType", StringType, true),
    StructField("CallDate", StringType, true),
    StructField("WatchDate", StringType, true),
    StructField("CallFinalDisposition", StringType, true),
    StructField("AvailableDtTm", StringType, true),
    StructField("Address", StringType, true),
    StructField("City", StringType, true),
    StructField("Zipcode", IntegerType, true),
    StructField("Battalion", StringType, true),
    StructField("StationArea", StringType, true),
    StructField("Box", StringType, true),
    StructField("OriginalPriority", StringType, true),
    StructField("Priority", StringType, true),
    StructField("FinalPriority", IntegerType, true),
    StructField("ALSUnit", BooleanType, true),
    StructField("CallTypeGroup", StringType, true),
    StructField("NumAlarms", IntegerType, true),
    StructField("UnitType", StringType, true),
    StructField("UnitSequenceInCallDispatch", IntegerType, true),
    StructField("FirePreventionDistrict", StringType, true),
    StructField("SupervisorDistrict", StringType, true),
    StructField("Neighborhood", StringType, true),
    StructField("Location", StringType, true),
    StructField("RowID", StringType, true),
    StructField("Delay", FloatType, true)))

    val path="data/sf-fire/sf-fire-calls.csv"

    var fireDF=spark.read.format("csv")
      .option("header", "true")
      .schema(fireSchema)
      .load(path)

      fireDF.show(5)

     fireDF.printSchema()

//    # ---------- Projection Operations -----------------
//    #A projection in relational parlance is a way to
//    return only the
//    #rows matching a certain relational condition by using filters

    var fewFireDF = fireDF.
          select("IncidentNumber", "AvailableDtTm", "CallType")
          .where("CallType != 'Medical Incident'")

    fewFireDF.show(false)

//    # ---------- Aggregation -----------------
//
//    #We want to know how many distinct CallTypes were recorded as the causes
//    #of the fire calls ?

    fireDF.
    select("CallType")
    .where(col("CallType").isNotNull)
    .agg(countDistinct("CallType").alias("Distinct Call Types"))
    .show()

//    #We want to know how many distinct CallTypes are there ?

    fireDF.
    select("CallType")
    .where(col("CallType").isNotNull)
    .distinct()
    .sort(col("CallType").desc)
    .show(30, truncate = false)

//    # ---------- Renaming , Adding and Dropping Columns --------------

    var newFireDF=fireDF.withColumnRenamed("Delay","ResponseDelayedInMins")

    newFireDF
    .select("ResponseDelayedInMins")
    .where(col("ResponseDelayedInMins") > 5)
    .show()

//    # COnvert the string dates to timestamps

    newFireDF
      .select("CallDate","WatchDate","AvailableDtTm")
      .show(5,truncate=false)

//    #spark.sql.functions has a set of to / from date / timestamp functions
//    #such as to_timestamp() and to_date()
    var fireTSDF = newFireDF
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .withColumn("AvailableDateTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))

    fireTSDF.printSchema()

    fireTSDF
      .select("CallDate","IncidentDate","WatchDate","OnWatchDate","AvailableDtTm","AvailableDateTS")
      .show(5,truncate=false)

//    # ---- Drop the columns ---------

    fireTSDF = fireTSDF
    .drop("CallDate")
    .drop("WatchDate")
    .drop("AvailableDtTm")

    fireTSDF.printSchema()

//    # ------------ Time Stamp Functions -------
//    #How many years of data we have
    fireTSDF
    .select(year(col("IncidentDate")))
    .distinct()
    .orderBy(year(col("IncidentDate")).desc)
    .show()

//    # ------------- Aggregation ---
//
//    #I want to find most common types of fire call

    fireTSDF
    .select("CallType")
    .where(col("CallType").isNotNull)
    .groupBy("CallType")
    .count()
    .orderBy(desc("count"))
    .show(30, truncate = false)

//    #Print the zipcodes in order of the calls they get
//     go from lowest to highest

    fireTSDF
    .select("Zipcode", "CallType")
    .where(col("Zipcode").isNotNull )
    .where(col("CallType").isNotNull )
    .groupBy("Zipcode", "CallType")
    .count()
    .orderBy("count")
    .show(1000, truncate = false)


    fireTSDF
    .select(sum("NumAlarms"), avg("ResponseDelayedInMins"),
      min ("ResponseDelayedInMins"), max("ResponseDelayedInMins")
    ).show()
//    # ------------- Fire The Queries ------------------
    fireTSDF.createOrReplaceTempView("fire_calls")


    var query = """Select CallType,COUNT(CallType)
                  |      from fire_calls
                  |    where CallType IS NOT NULL
                  |      GROUP BY CallType
                  |      ORDER BY COUNT(calltype)""".stripMargin




    spark.sql(query).show()

    var query1 = """Select COUNT(*)
                  |      from fire_calls 
                  |    where Zipcode = 94118""".stripMargin



    spark.sql(query1).show()



    Thread.sleep(240000)
  }
}




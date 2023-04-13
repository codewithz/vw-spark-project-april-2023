Spark SQL

Starting a Shell

spark2-shell 

Data in HDFS -->

From HDFS to RDD and from RDD to DataFrame 

var myRdd=sc.textFile("/user/zartab_639001/emp.txt")

Data is converted to RDD.ABS

--> Create a Schema so that the it can be applied on the RDD 

case class Employee(empId:Int, name:String,age:Int,salary:Float,dept:String);


var splitRdd=myRdd.map(x=>x.split(","));

Array[Array[String]] = Array(Array(1, Sheldon, 30, 5000.0, IT), Array(2, Leonard, 29, 3500.0, HR), Array(3, Penny, 29, 2300.0, IT), A
rray(4, Raj, 32, 3200.0, HR), Array(5, Howard, 31, 4300.0, Finance), Array(6, Bernadate, 29, 3500.0, IT), Array(7, Amy, 30, 3210.0, Finance
))

var cRdd=splitRdd.map(x=>Employee(x(0).toInt,x(1),x(2).toInt,x(3).toFloat,x(4)));


cRdd.collect
res7: Array[Employee] = Array(Employee(1,Sheldon,30,5000.0,IT), Employee(2,Leonard,29,3500.0,HR), Employee(3,Penny,29,2300.0,IT), Employee(
4,Raj,32,3200.0,HR), Employee(5,Howard,31,4300.0,Finance), Employee(6,Bernadate,29,3500.0,IT), Employee(7,Amy,30,3210.0,Finance))


var myDF=cRdd.toDF

myDF.show
+-----+---------+---+------+-------+
|empId|     name|age|salary|   dept|
+-----+---------+---+------+-------+
|    1|  Sheldon| 30|5000.0|     IT|
|    2|  Leonard| 29|3500.0|     HR|
|    3|    Penny| 29|2300.0|     IT|
|    4|      Raj| 32|3200.0|     HR|
|    5|   Howard| 31|4300.0|Finance|
|    6|Bernadate| 29|3500.0|     IT|
|    7|      Amy| 30|3210.0|Finance|


myDF.select("name","age").show

-- Applying Filter 


myDF.filter("dept='IT'").show

myDF.select("name","age","dept","salary").filter("dept='IT'").show

myDF.select("name","age","dept","salary").filter("dept='IT' and salary>3500").show

myDF.select("name","age","dept","salary").filter("dept='IT' or dept='HR'").show

myDF.select("name","age","dept","salary").where("dept='IT' or dept='HR'").show

-- Sorting 

myDF.sort("dept").show

myDF.orderBy(col("dept")).show

myDF.orderBy(col("dept").desc).show

myDF.orderBy(col("dept").desc,col("salary")).show

-- Expression 

-- Update employee set salary=salary+(salary*.25) where dept='IT';

myDF.select(col("name"),expr("salary* 1.1") as "10% hiked salary").show

myDF.select(expr("name"),expr("salary+1500"),expr("name like 'P%'") as "Does name starts with P?").show


-> LOADING DATA USING SparkSession object. 

var myDF=spark.read.format("csv").option("delimiter",",").option("header","false").load("/user/zartab_639001/emp.txt")



var myDF=spark.read.format("csv").option("delimiter",",").option("inferSchema","true").option("header","false").load("/user/zartab_639001/emp.txt")


yDF=myDF.toDF("empId","name","age","salary","dept")
myDF: org.apache.spark.sql.DataFrame = [empId: int, name: string ... 3 more fields]
scala> myDF.show
+-----+---------+---+------+-------+
|empId|     name|age|salary|   dept|
+-----+---------+---+------+-------+
|    1|  Sheldon| 30|5000.0|     IT|
|    2|  Leonard| 29|3500.0|     HR|
|    3|    Penny| 29|2300.0|     IT|
|    4|      Raj| 32|3200.0|     HR|
|    5|   Howard| 31|4300.0|Finance|
|    6|Bernadate| 29|3500.0|     IT|
|    7|      Amy| 30|3210.0|Finance|
+-----+---------+---+------+-------+
scala> myDF.printSchema
root
 |-- empId: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- salary: double (nullable = true)
 |-- dept: string (nullable = true)

 -> Group BY

 Select dept,COUNT(empID)
 from employee
 group by dept 

 myDF.groupBy("dept").agg(count(col("empId"))).show

 myDF.groupBy("dept").agg(count(col("empId")),sum(col("salary"))).show

 myDF.groupBy("dept","age").agg(count(col("empID")),sum(col("salary"))).show

 myDF.groupBy("dept").agg(Map("salary"->"sum","empId"->"count")).show

 -> LOADING DATA as Dataset 

 case class Cricketer(name:String,position:String)

 var c=spark.read.json("/user/zartab_639001/cricket.json")

 var cDS=spark.read.json("/user/zartab_639001/cricket.json").as[Cricketer]

 -- Text Analysis on Data 

 AFFINN 

 var tweetsArray=Array("2020 was an awesome year for personal development",
 "2020 was the worst year ever","2020 was good for me as well as bad for me")

var myRdd=sc.parallelize(tweetsArray)

var tweetsDF=myRdd.toDF("tweets")


tweetsDF.explode("tweets","word"){x:String=>x.split(" ")}.show(false)
warning: there was one deprecation warning; re-run with -deprecation for details
+-------------------------------------------------+-----------+
|tweets                                           |word       |
+-------------------------------------------------+-----------+
|2020 was an awesome year for personal development|2020       |
|2020 was an awesome year for personal development|was        |
|2020 was an awesome year for personal development|an         |
|2020 was an awesome year for personal development|awesome    |
|2020 was an awesome year for personal development|year       |
|2020 was an awesome year for personal development|for        |
|2020 was an awesome year for personal development|personal   |
|2020 was an awesome year for personal development|development|
|2020 was the worst year ever                     |2020       |
|2020 was the worst year ever                     |was        |
|2020 was the worst year ever                     |the        |
|2020 was the worst year ever                     |worst      |
|2020 was the worst year ever                     |year       |
|2020 was the worst year ever                     |ever       |
|2020 was good for me as well as bad for me       |2020       |
|2020 was good for me as well as bad for me       |was        |
|2020 was good for me as well as bad for me       |good       |
|2020 was good for me as well as bad for me       |for        |
|2020 was good for me as well as bad for me       |me         |
|2020 was good for me as well as bad for me       |as         |
+-------------------------------------------------+-----------+
 

 --Loading Data in JSON 

 df --> json data 

 
var data=Array("""{"Name":"Alex","Age":"25","CoWorkers":["Mike","John"],"Address":{"city":"Mumbai","state":"MH","country":"India","pincode":"400004"}}""",
"""{"Name":"Mike","Age":"30","CoWorkers":["Alex","John"],"Address":{"city":"Bengaluru","state":"KA","country":"India"}}"""
)


scala> var df=sc.parallelize(data).toDF("value")
df: org.apache.spark.sql.DataFrame = [value: string]
scala> 
scala> df.show(false)
+------------------------------------------------------------------------------------------------------------------------------------+
|value                                                                                                                               |
+------------------------------------------------------------------------------------------------------------------------------------+
|{"Name":"Alex","Age":"25","CoWorkers":["Mike","John"],"Address":{"city":"Mumbai","state":"MH","country":"India","pincode":"400004"}}|
|{"Name":"Mike","Age":"30","CoWorkers":["Alex","John"],"Address":{"city":"Bengaluru","state":"KA","country":"India"}}                |
+------------------------------------------------------------------------------------------------------------------------------------+
scala> df.withColumn("name",get_json_object(col("value"),"$.Name")).show
 
 
 df.withColumn("name",get_json_object(col("value"),"$.Name")).withColumn("city",get_json_object(col("value"),"$.Address.city")).show
+--------------------+----+---------+
|               value|name|     city|
+--------------------+----+---------+
|{"Name":"Alex","A...|Alex|   Mumbai|
|{"Name":"Mike","A...|Mike|Bengaluru|
+--------------------+----+---------+

 df.withColumn("name",get_json_object(col("value"),"$.Name")).withColumn("city",get_json_object(col("value"),"$.Address.city")).drop("value").drop("value").show


var tempDF=
 df.withColumn("name",get_json_object(col("value"),"$.Name")).withColumn("city",get_json_object(col("value"),"$.Address.city")).drop("value").drop("value")


 -- Using SQL Queries to fetch the details.

 Read the data from JSON 

 var df=spark.read.json("/user/zartab_639001/employee.json")


 scala> var df=spark.read.json("/user/zartab_639001/employee.json")
df: org.apache.spark.sql.DataFrame = [age: bigint, name: string]                
scala> df.show
+---+-------+
|age|   name|
+---+-------+
| 28|   John|
| 36|   Andy|
| 22| Clarke|
| 42|  Kevin|
| 51|Richard|
+---+-------+
scala> df.printSchema
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)
scala> df.createOrReplaceTempView("emp")

scala> var query="Select * from emp";
query: String = Select * from emp
scala> spark.sql(query).show
+---+-------+
|age|   name|
+---+-------+
| 28|   John|
| 36|   Andy|
| 22| Clarke|
| 42|  Kevin|
| 51|Richard|

var players=spark.read.format("csv")
          .option("header","true")
          .option("inferSchema","true")
          .load("/user/zartab_639001/players.txt")

players: org.apache.spark.sql.DataFrame = [id: int, name: string ... 1 more field]

scala> players.show
+---+----------------+------------+
| id|            name|     country|
+---+----------------+------------+
|  1|Sachin Tendulkar|       India|
|  2|   M Murlidharan|   Sri Lanka|
|  3|     Wasim Akram|    Pakistan|
|  4|   AB D Villiers|South Africa|
|  5|     Virat Kohli|       India|
+---+----------------+------------+


var records=spark.read.format("csv")
          .option("header","true")
          .option("inferSchema","true")
          .load("/user/zartab_639001/records.txt")

scala> players
        .join(records,players.col("id")===records.col("pid"))
        .show(false)
+---+----------------+------------+---+------------------------+---+
|id |name            |country     |id |record                  |pid|
+---+----------------+------------+---+------------------------+---+
|1  |Sachin Tendulkar|India       |2  |Most Runs in ODI        |1  |
|1  |Sachin Tendulkar|India       |1  |Most Centuries in ODI   |1  |
|2  |M Murlidharan   |Sri Lanka   |3  |Most Wickets in ODI     |2  |
|3  |Wasim Akram     |Pakistan    |4  |Most Hatricks in Cricket|3  |
|4  |AB D Villiers   |South Africa|6  |Fastest 50 in ODI       |4  |
|4  |AB D Villiers   |South Africa|5  | Fastest 100 in ODI     |4  |
+---+----------------+------------+---+------------------------+---+

scala> players
      .join(records,players.col("id")===records.col("pid"),"left_outer")
      .show(false)
+---+----------------+------------+----+------------------------+----+
|id |name            |country     |id  |record                  |pid |
+---+----------------+------------+----+------------------------+----+
|1  |Sachin Tendulkar|India       |2   |Most Runs in ODI        |1   |
|1  |Sachin Tendulkar|India       |1   |Most Centuries in ODI   |1   |
|2  |M Murlidharan   |Sri Lanka   |3   |Most Wickets in ODI     |2   |
|3  |Wasim Akram     |Pakistan    |4   |Most Hatricks in Cricket|3   |
|4  |AB D Villiers   |South Africa|6   |Fastest 50 in ODI       |4   |
|4  |AB D Villiers   |South Africa|5   | Fastest 100 in ODI     |4   |
|5  |Virat Kohli     |India       |null|null                    |null|
+---+----------------+------------+----+------------------------+----+
scala> players
      .join(records,players.col("id")===records.col("pid"))
      .select("name","country","record")
      .show(false)
+----------------+------------+------------------------+
|name            |country     |record                  |
+----------------+------------+------------------------+
|Sachin Tendulkar|India       |Most Runs in ODI        |
|Sachin Tendulkar|India       |Most Centuries in ODI   |
|M Murlidharan   |Sri Lanka   |Most Wickets in ODI     |
|Wasim Akram     |Pakistan    |Most Hatricks in Cricket|
|AB D Villiers   |South Africa|Fastest 50 in ODI       |
|AB D Villiers   |South Africa| Fastest 100 in ODI     |
+----------------+------------+------------------------+

scala> players
        .join(records,players.col("id")===records.col("pid"))
        .groupBy(players.col("country"))
        .agg(count(records.col("id"))).show
+------------+---------+                                                        
|     country|count(id)|
+------------+---------+
|   Sri Lanka|        1|
|       India|        2|
|South Africa|        2|
|    Pakistan|        1|
+------------+---------+

players.createOrReplaceTempView("players");

records.createOrReplaceTempView("records");

scala> var q1="Select p.name,p.country,r.record from players p join records r on p.id=r.pid";
q1: String = Select p.name,p.country,r.record from players p join records r on p.id=r.pid
scala> spark.sql(q1)
res13: org.apache.spark.sql.DataFrame = [name: string, country: string ... 1 more field]
scala> spark.sql(q1).show
+----------------+------------+--------------------+
|            name|     country|              record|
+----------------+------------+--------------------+
|Sachin Tendulkar|       India|    Most Runs in ODI|
|Sachin Tendulkar|       India|Most Centuries in...|
|   M Murlidharan|   Sri Lanka| Most Wickets in ODI|
|     Wasim Akram|    Pakistan|Most Hatricks in ...|
|   AB D Villiers|South Africa|   Fastest 50 in ODI|
|   AB D Villiers|South Africa|  Fastest 100 in ODI|
+----------------+------------+--------------------+


scala> var q2="Select p.name,p.country,r.record from players p join records r on p.id=r.pid where p.country='India'";
q2: String = Select p.name,p.country,r.record from players p join records r on p.id=r.pid where p.country='India'
scala> spark.sql(q2).show
+----------------+-------+--------------------+
|            name|country|              record|
+----------------+-------+--------------------+
|Sachin Tendulkar|  India|    Most Runs in ODI|
|Sachin Tendulkar|  India|Most Centuries in...|
+----------------+-------+--------------------+

var q3="Select p.country,count(r.record) from players p join records r on p.id=r.pid group by p.country";

-- Set Operations 

var set1DF=spark.read.format("csv").option("delimiter",",").option("header","true").load("/user/zartab_639001/set1.txt")

var set2DF=spark.read.format("csv").option("delimiter",",").option("header","true").load("/user/zartab_639001/set2.txt")


set1DF.union(set2DF).show

scala> set1DF.intersect(set2DF).show
+---+-----+                                                                     
| id|value|
+---+-----+
|  4|   28|
|  1|   23|
|  3|   56|
+---+-----+
scala> set1DF.except(set2DF).show
+---+-----+
| id|value|
+---+-----+
|  2|   45|
|  5|   71|
+---+-----+
scala> set2DF.except(set1DF).show
+---+-----+
| id|value|
+---+-----+
|  6|   87|
|  7|   28|
+---+-----+

var unionDF=set1DF.union(set2DF)

unionDF.groupBy("id").agg(count(col("id")).as("count")).filter("count>1").show

-- *** Analytical Functions ---- *** 

-> Windows Functions 

-- Loading the Data 

var myDF=spark.read.format("csv").option("delimiter",",").option("header","true").option("inferSchema","true").load("/user/zartab_639001/productRevenue.csv")


ar myDF=spark.read.format("csv").option("delimiter",",").option("header","true").option("inferSchema","true").load("/user/zartab_6
39001/productRevenue.csv")
myDF: org.apache.spark.sql.DataFrame = [product: string, category: string ... 1 more field]
scala> myDF.show
+----------+----------+-------+
|   product|  category|revenue|
+----------+----------+-------+
|      Thin|Cell Phone|   6000|
|    Normal|    Tablet|   1500|
|      Mini|    Tablet|   5500|
|Ultra Thin|Cell Phone|   5000|
| Very Thin|Cell Phone|   6000|
|       Big|    Tablet|   2500|
|  Bendable|Cell Phone|   3000|
|  Foldable|Cell Phone|   3000|
|       Pro|    Tablet|   4500|
|      Pro2|    Tablet|   6500|
+----------+----------+-------+


Q1. What are the best selling and second best selling products in each category? 

import org.apache.spark.sql.expressions

var partition=expressions.Window.partitionBy(col("category"))

myDF.withColumn("HighestRevenue",row_number() over partition.orderBy(col("revenue").desc)).sort("category").
filter("HighestRevenue<=2").show

myDF.withColumn("HighestRevenue",row_number() over partition.orderBy(col("revenue").desc))
.sort("category")
.filter("HighestRevenue<=3")
.show

Q2. What is the difference between the revenue of each product and best selling product in each category?

import org.apache.spark.sql.functions 

var partitionDF=expressions.Window.partitionBy(col("category")).orderBy(col("revenue").desc)
var revenueDiff=(functions.max(col("revenue")).over(partitionDF)-col("revenue"))

myDF.select(col("product"),col("category"),col("revenue"),revenueDiff.alias("difference")).show

+----------+----------+-------+----------+
|   product|  category|revenue|difference|
+----------+----------+-------+----------+
|      Pro2|    Tablet|   6500|         0|
|      Mini|    Tablet|   5500|      1000|
|       Pro|    Tablet|   4500|      2000|
|       Big|    Tablet|   2500|      4000|
|    Normal|    Tablet|   1500|      5000|
|      Thin|Cell Phone|   6000|         0|
| Very Thin|Cell Phone|   6000|         0|
|Ultra Thin|Cell Phone|   5000|      1000|
|  Bendable|Cell Phone|   3000|      3000|
|  Foldable|Cell Phone|   3000|      3000|
+----------+----------+-------+----------+

_------------------------------------------------------------------------------

var empDF=spark.read.format("csv").option("header","false").option("delimiter",",").option("inferSchema","true").load("/user/zartab_639001/emp.txt").toDF("empId","name","age","salary","dept")

+-----+---------+---+------+-------+
|empId|     name|age|salary|   dept|
+-----+---------+---+------+-------+
|    1|  Sheldon| 30|5000.0|     IT|
|    2|  Leonard| 29|3500.0|     HR|
|    3|    Penny| 29|2300.0|     IT|
|    4|      Raj| 32|3200.0|     HR|
|    5|   Howard| 31|4300.0|Finance|
|    6|Bernadate| 29|3500.0|     IT|
|    7|      Amy| 30|3210.0|Finance|
+-----+---------+---+------+-------+

var deptPartition=expressions.Window.partitionBy(col("dept"))

empDF.withColumn("OldestPerson",row_number() over deptPartition.orderBy(col("age").desc)).sort("dept")
.filter("OldestPerson<=2").show


empDF.createOrReplaceTempView("employee")

val q="Select name,age,salary,dept,row_number() over (partition by dept order by salary desc) rank from employee"

val results=spark.sql(q)

scala> results.show
+---------+---+------+-------+----+
|     name|age|salary|   dept|rank|
+---------+---+------+-------+----+
|  Leonard| 29|3500.0|     HR|   1|
|      Raj| 32|3200.0|     HR|   2|
|   Howard| 31|4300.0|Finance|   1|
|      Amy| 30|3210.0|Finance|   2|
|  Sheldon| 30|5000.0|     IT|   1|
|Bernadate| 29|3500.0|     IT|   2|
|    Penny| 29|2300.0|     IT|   3|
+---------+---+------+-------+----+


-- Utility Queries ---

1. Storing data in HDFS 

results.write.format("csv").option("delimiter",",").option("header","true").save("/user/zartab_639001/sparkop_b9/employee_rank.csv")


2. Loading the Data in ORC Format 

results.write.format("orc").save("/user/zartab_639001/sparkop_b9/employee_rank.orc")

3. Storing data using PARTITION

  SQL ----

spark.udf.register("IncBy5k",(salary:Double)=>(salary+5000))

empDF.createOrReplaceTempView("employee")

var q="Select name,age,salary, IncBy5k(salary) as PredictedSalary from employee"

var results=spark.sql(q)
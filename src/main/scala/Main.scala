import org.apache.spark._
import org.apache.log4j._

object HelloWorld {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "HelloWorld")

    val lines = sc.textFile("data/1800.csv")
    val numLines = lines.count()

    println("Hello world! The u.data file has " + numLines + " lines.")

   Thread.sleep(240000)
    sc.stop()
  }
}
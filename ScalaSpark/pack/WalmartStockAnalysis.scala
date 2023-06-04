package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object WalmartStockAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Walmart-Stock-Analysis")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
      "C:/Users/Mohan/Downloads/WMT.csv")
    df.show()

    val df1 = df.count()
    println(df1)

    // To find the column names
    val coldf = df.columns
    coldf.foreach(println)

    // To print the schema
    df.printSchema()

    //To print the first 5 lines
    df.show(5)

    // Use describe() to learn about the DataFrame.
    val descdf = df.describe()
    descdf.show()

    // There are too many decimal places for mean and stddev in the describe() dataframe. Format the numbers to just show up to two decimal places
    val decdf = descdf.filter(col("summary").isin("mean", "stddev", "min", "max"))
    decdf.show()

    val findecdf = decdf.withColumn("Open", col("Open").cast("decimal(10,2)")).withColumn("High", col("High").cast("decimal(10,2)"))
      .withColumn("Low", col("Low").cast("decimal(10,2)"))
      .withColumn("Close", col("Close").cast("decimal(10,2)"))
      .withColumn("Adj Close", col("Adj Close").cast("decimal(10,2)"))
      .withColumn("Close", col("Close").cast("decimal(10,2)"))
      .withColumn("Volume", col("Volume").cast("decimal(10,2)"))
    findecdf.show()

    // Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.
    val hvdf = df.withColumn("HV Ratio", expr("High/Volume"))
    hvdf.show(5, false)

    // What is the mean of the Close column?
    val meandf = df.select(mean("Close").as("mean").cast("decimal(10,2)"))
    meandf.show()

    // What is the max and min of the Volume column?
    val maxdf = df.select(max("Volume").alias("MaxVolume"), min("Volume").alias("MinVolume"))
    maxdf.show()

    // How many days was the Close lower than 60 dollars?
    val closecountdf = df.filter(col("Close") < 60).count()
    println(closecountdf)

    // What percentage of the time was the High greater than 80 dollars ?
    val highpercdf = df.filter(col("High") > 80).count() * 100 / df1
    println(highpercdf)

    // What is the max High per year?
    val maxhighdf = df.withColumn("year", year(col("Date"))).groupBy("year").agg(
      max("High").cast("decimal(10,2)").alias("High")).orderBy("year")
    maxhighdf.show()

    // What is the average Close for each Calendar Month?
    val avgclosedf = df.withColumn("month", month(col("Date"))).groupBy("month").agg(
      avg("Close").cast("decimal(10,2)").alias("average")).orderBy("month")
    avgclosedf.show()
  }
}

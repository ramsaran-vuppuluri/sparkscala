import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, expr, pow}

object LearningCurve extends App {
  val spark: SparkSession = LoadConfig(appName = "LearningCurve").getSparkSession

  //manualSchema

  //manualDfCreation

  val invoiceInfoCsv = spark.read.option("infereSchema", "true").option("header", "true").csv("/Users/saha/Documents/Big Data/Spark/Data/data/retail-data/by-day/2010-12-01.csv")

  val DOTCodeFilter = col("StockCode") === "DOT"
  val priceFilter = col("UnitPrice") > 600
  val descriptionFilter = col("Description").contains("POSTAGE")

  invoiceInfoCsv.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descriptionFilter))).where("isExpensive").show

  invoiceInfoCsv.where(col("Description").eqNullSafe("Hello")).show

  val fabricatedQuantity = pow(col("Quantity") * col("Quantity"), 2)+5
  invoiceInfoCsv.select(expr("CustomerID"),fabricatedQuantity.alias("realQuantity")).show

  private def manualDfCreation = {
    val schema = new StructType(Array(StructField("some", StringType, true), StructField("col", StringType, true), StructField
    ("name", LongType, true)))

    val myRow = Seq(Row("Hello", null, 1L))
    val myRdd = spark.sparkContext.parallelize(myRow)
    val myDf = spark.createDataFrame(myRdd, schema)
    myDf.show
  }


  private def manualSchema = {
    val flighDataJsonSchema = StructType(List(StructField("ORIGIN_COUNTRY_NAME", StringType, true), StructField
    ("DEST_COUNTRY_NAME", StringType, true), StructField("count", LongType, true)))

    spark.read.json("/Users/saha/Documents/Big Data/Spark/Data/data/flight-data/json/2015-summary.json").schema

    spark.read.schema(flighDataJsonSchema).json("/Users/saha/Documents/Big " +
      "Data/Spark/Data/data/flight-data/json/2015-summary.json").show
  }

}
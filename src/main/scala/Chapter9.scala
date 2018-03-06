import java.util.Properties

import SparkIOEnum.{Format, SQLOptions}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Chapter9 extends App {
  val DEST_COUNTRY_NAME = "DEST_COUNTRY_NAME"
  val ORIGIN_COUNTRY_NAME = "ORIGIN_COUNTRY_NAME"
  val COUNT = "count"

  val driver = "org.sqlite.JDBC"
  val path = "/Users/saha/Documents/Big Data/Spark/Data/data/flight-data/jdbc/my-sqlite.db"
  val url = s"jdbc:sqlite:/${path}"
  val tableName = "flight_info"

  val spark = LoadConfig.apply(appName = "Chapter9").getSparkSession

  val myManualSchema = new StructType(Array(new StructField(DEST_COUNTRY_NAME, StringType, true), new StructField(ORIGIN_COUNTRY_NAME, StringType, true), new StructField(COUNT, LongType, true)))

  val flightData2010Csv = spark.read.format(Format.CSV.getFormat).option("header", "true").option("mode", "FAILFAST").schema(myManualSchema).load("/Users/saha/Documents/Big Data/Spark/Data/data/flight-data/csv/2010-summary.csv")

  //csvOperations

  //jsonOperations

  //parquetOperatios

  //dbOperations

  //textFileOperations

  flightData2010Csv.repartition(5).write.format("csv").mode(SaveMode.Overwrite.toString).save("/tmp/multiple.csv")

  flightData2010Csv.limit(10).write.mode(SaveMode.Overwrite.toString).partitionBy(DEST_COUNTRY_NAME).save("/tmp/partitioned-files.parquet")

  flightData2010Csv.write.format("parquet").mode(SaveMode.Overwrite.toString).bucketBy(10, COUNT).saveAsTable("bucketFiles")

  def textFileOperations = {
    spark.read.textFile("/Users/saha/Documents/Big Data/Spark/Data/data/flight-data/csv/2010-summary.csv").selectExpr("split(value,',')").show(5, false)
    /*Output of above statement
    18/01/09 05:44:50 INFO CodeGenerator: Code generated in 38.431757 ms
    +-----------------------------------------------+
    |split(value, ,)                                |
    +-----------------------------------------------+
    |[DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count]|
    |[United States, Romania, 1]                    |
    |[United States, Ireland, 264]                  |
    |[United States, India, 69]                     |
    |[Egypt, United States, 24]                     |
    +-----------------------------------------------+
    only showing top 5 rows
     */

    flightData2010Csv.select(DEST_COUNTRY_NAME).write.text("/tmp/simple-text-file.txt")

    flightData2010Csv.limit(10).select(DEST_COUNTRY_NAME, COUNT).write.partitionBy(COUNT).text("/tmp/five-csv-files2.csv")
  }

  private def parquetOperatios = {
    spark.read.format("parquet").load("/Users/saha/Documents/Big Data/Spark/Data/data/flight-data/parquet/2010-summary.parquet/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet").show(5)
    /*Output of above statement
    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Romania|    1|
    |    United States|            Ireland|  264|
    |    United States|              India|   69|
    |            Egypt|      United States|   24|
    |Equatorial Guinea|      United States|    1|
    +-----------------+-------------------+-----+
    only showing top 5 rows
     */

    flightData2010Csv.write.format("parquet").mode(SaveMode.Overwrite).save("/tmp/my-parquet-file.parquet")
    /*Output of above statement
    18/01/09 05:31:03 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
    {
      "type" : "struct",
      "fields" : [ {
        "name" : "DEST_COUNTRY_NAME",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "ORIGIN_COUNTRY_NAME",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "count",
        "type" : "long",
        "nullable" : true,
        "metadata" : { }
      } ]
    }
    and corresponding Parquet message type:
    message spark_schema {
      optional binary DEST_COUNTRY_NAME (UTF8);
      optional binary ORIGIN_COUNTRY_NAME (UTF8);
      optional int64 count;
    }
     */
  }


  private def jsonOperations = {
    val flightData2010Json = spark.read.format("json").option("mode", "FAILFAST").schema(myManualSchema).load("/Users/saha/Documents/Big Data/Spark/Data/data/flight-data/json/2010-summary.json")

    flightData2010Json.show(5)
    /*Output of above statement
    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Romania|    1|
    |    United States|            Ireland|  264|
    |    United States|              India|   69|
    |            Egypt|      United States|   24|
    |Equatorial Guinea|      United States|    1|
    +-----------------+-------------------+-----+
    only showing top 5 rows
     */

    flightData2010Json.write.format("json").mode(SaveMode.Overwrite.toString).save("/tmp/my-json-file.json")
  }

  private def csvOperations {
    flightData2010Csv.show(5, false)
    /*Output of above statement
    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |United States    |Romania            |1    |
    |United States    |Ireland            |264  |
    |United States    |India              |69   |
    |Egypt            |United States      |24   |
    |Equatorial Guinea|United States      |1    |
    +-----------------+-------------------+-----+
    only showing top 5 rows
    */

    flightData2010Csv.write.format(Format.CSV.getFormat).mode(SaveMode.Overwrite.toString).option("sep", "\t").save("/tmp/my-tsv-file.tsv")
  }

  private def dbOperations = {
    //coreDbIo

    //pushDownQuery

    //partitionQuery

    val newPath = "jdbc:sqlite://tmp/my-sqlite.db"
    val props = new Properties
    props.setProperty(SQLOptions.DRIVER.getSqlOption, driver)

    flightData2010Csv.write.mode("overwrite").jdbc(newPath, tableName, props)

    println(spark.read.jdbc(newPath, tableName, props).count)
    /*Output of above statement
    255
     */

    flightData2010Csv.write.mode("append").jdbc(newPath, tableName, props)

    println(spark.read.jdbc(newPath, tableName, props).count)
    /*Output of above statement
    510
     */
  }

  private def partitionQuery = {
    val flighDataDbDf = spark.read.format(Format.JDBC.getFormat).option(SQLOptions.URL.getSqlOption, url).option(SQLOptions.DRIVER.getSqlOption, driver).option(SQLOptions.DB_TABLE.getSqlOption, tableName).option(SQLOptions.NUM_PARTITIONS.getSqlOption, 10).load

    flighDataDbDf.select(DEST_COUNTRY_NAME).distinct.show(5, false)
    /*Output of above statement
    +-----------------+
    |DEST_COUNTRY_NAME|
    +-----------------+
    |Anguilla         |
    |Russia           |
    |Paraguay         |
    |Senegal          |
    |Sweden           |
    +-----------------+
    only showing top 5 rows
    */

    val props = new Properties
    props.setProperty(SQLOptions.DRIVER.getSqlOption, driver)

    val predicates = Array("DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
      "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")

    spark.read.jdbc(url, tableName, predicates, props).show
    /*Output of above statement
    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |           Sweden|      United States|   65|
    |    United States|             Sweden|   73|
    |         Anguilla|      United States|   21|
    |    United States|           Anguilla|   20|
    +-----------------+-------------------+-----+
    */

    val lowerBound = 0L
    val upperBound = 348113L
    val numPartitions = 10

    println(spark.read.jdbc(url, tableName, COUNT, lowerBound, upperBound, numPartitions, props).count)
    /*Output of above statement
    255
    */
  }


  private def pushDownQuery = {
    val pushDownQuery = "(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info) AS flight_info"

    val flightDataDbDfPushDown = spark.read.format(Format.JDBC.getFormat).option(SQLOptions.URL.getSqlOption, url).option(SQLOptions.DRIVER.getSqlOption, driver).option(SQLOptions.DB_TABLE.getSqlOption, pushDownQuery).load

    flightDataDbDfPushDown.explain
    /*Output of above statement
    == Physical Plan ==
    *Scan JDBCRelation((SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info) AS flight_info) [numPartitions=1] [DEST_COUNTRY_NAME#0] ReadSchema: struct<DEST_COUNTRY_NAME:string>
    */

    flightDataDbDfPushDown.show(5, false)
    /*Output of above statement
    +-----------------+
    |DEST_COUNTRY_NAME|
    +-----------------+
    |United States    |
    |Egypt            |
    |Equatorial Guinea|
    |Costa Rica       |
    |Senegal          |
    +-----------------+
    only showing top 5 rows
    */
  }

  private def coreDbIo = {
    val flightDataDbDf = spark.read.format(Format.JDBC.getFormat).option(SQLOptions.URL.getSqlOption, url).option(SQLOptions.DB_TABLE.getSqlOption, tableName).option(SQLOptions.DRIVER.getSqlOption, driver).load

    flightDataDbDf.select(DEST_COUNTRY_NAME).distinct.explain
    /*Output of above statement
    == Physical Plan ==
    *HashAggregate(keys=[DEST_COUNTRY_NAME#0], functions=[])
    +- Exchange hashpartitioning(DEST_COUNTRY_NAME#0, 200)
    +- *HashAggregate(keys=[DEST_COUNTRY_NAME#0], functions=[])
    +- *Scan JDBCRelation(flight_info) [numPartitions=1] [DEST_COUNTRY_NAME#0] ReadSchema: struct<DEST_COUNTRY_NAME:string>
    */

    flightDataDbDf.select(DEST_COUNTRY_NAME).distinct.show(5, truncate = false)
    /*Output of above statement
    +-----------------+
    |DEST_COUNTRY_NAME|
    +-----------------+
    |Anguilla         |
    |Russia           |
    |Paraguay         |
    |Senegal          |
    |Sweden           |
    +-----------------+
    only showing top 5 rows
    */

    flightDataDbDf.filter(DEST_COUNTRY_NAME + " in ('Anguilla','Sweden')").explain
    /*Output of above statement
    == Physical Plan ==
    *Scan JDBCRelation(flight_info) [numPartitions=1] [DEST_COUNTRY_NAME#0,ORIGIN_COUNTRY_NAME#1,count#2] PushedFilters: [*In(DEST_COUNTRY_NAME, [Anguilla,Sweden])], ReadSchema: struct<DEST_COUNTRY_NAME:string,ORIGIN_COUNTRY_NAME:string,count:decimal(20,0)>
    */

    flightDataDbDf.filter(DEST_COUNTRY_NAME + " in ('Anguilla','Sweden')").show(5, false)
    /*Output of above statement
    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |Anguilla         |United States      |21   |
    |Sweden           |United States      |65   |
    +-----------------+-------------------+-----+
    */
  }
}
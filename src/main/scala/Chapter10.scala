

object Chapter10 extends App {
  val DEST_COUNTRY_NAME = "DEST_COUNTRY_NAME"
  val ORIGIN_COUNTRY_NAME = "ORIGIN_COUNTRY_NAME"
  val COUNT = "count"

  val spark = LoadConfig(appName = "Chapter10").getSparkSession

  spark.sql("" +
    "CREATE TABLE IF NOT EXISTS flights" +
    "(" +
    "DEST_COUNTRY_NAME STRING, " +
    "ORIGIN_COUNTRY_NAME STRING COMMENT 'remember, the US will be most prevalent', " +
    "count Long" +
    ") " +
    "USING JSON OPTIONS " +
    "(path '/Users/saha/Documents/Big Data/Spark/Data/data/flight-data/json/2015-summary.json') PARTITIONED BY (DEST_COUNTRY_NAME)")

  spark.sql("" +
    "CREATE EXTERNAL TABLE hive_flights " +
    "(" +
    "DEST_COUNTRY_NAME STRING, " +
    "ORIGIN_COUNTRY_NAME STRING COMMENT 'remember, the US will be most prevalent', " +
    "count Long" +
    ")" +
    "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
    "LOCATION '/Users/saha/Documents/Big Data/Spark/Data/data/flight-data-hive'"
  )
}
import org.apache.spark.sql.Dataset

object Chapter11 extends App {
  val spark = LoadConfig(appName = "Chapter11").getSparkSession

  val sqlContext = spark.sqlContext

  import sqlContext.implicits._

  val flightDf = spark.read.parquet("/Users/saha/Documents/Big Data/Spark/Data/data/flight-data/parquet/2010-summary.parquet/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")

  val flightDs: Dataset[Flight] = flightDf.as[Flight]

  flightDs.show(2)
  /*Output of above statement
  +-----------------+-------------------+-----+
  |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
  +-----------------+-------------------+-----+
  |    United States|            Romania|    1|
  |    United States|            Ireland|  264|
  +-----------------+-------------------+-----+
  only showing top 2 rows
   */

  println(flightDs.first.DEST_COUNTRY_NAME)
  /*Output of above statement
  United States
   */

  flightDs.filter(flight_row => {
    flight_row.DEST_COUNTRY_NAME == flight_row.ORIGIN_COUNTRY_NAME
  }).show(truncate = false)
  /*Output of above statement
  +-----------------+-------------------+------+
  |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count |
  +-----------------+-------------------+------+
  |United States    |United States      |348113|
  +-----------------+-------------------+------+
   */

  flightDs.filter(flight_row => originIsDestination(flight_row)).show(truncate = false)
  /*Output of above statement
  +-----------------+-------------------+------+
  |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count |
  +-----------------+-------------------+------+
  |United States    |United States      |348113|
  +-----------------+-------------------+------+
   */

  flightDs.map(flight => flight.DEST_COUNTRY_NAME).show(5, false)
  /*Output of above statement
  +-----------------+
  |value            |
  +-----------------+
  |United States    |
  |United States    |
  |United States    |
  |Egypt            |
  |Equatorial Guinea|
  +-----------------+
  only showing top 5 rows
   */

  def originIsDestination(flight: Flight): Boolean = {
    flight.ORIGIN_COUNTRY_NAME == flight.DEST_COUNTRY_NAME
  }
}

case class FlightMetaData()
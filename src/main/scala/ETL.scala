object ETL extends App {
  val spark = LoadConfig(appName = "ETL").getSparkSession

  val csvFile = spark.read.option("infereSchema", true).option("header", "true").csv("/Users/saha/Documents/Big Data/Spark/data/bike-data/201508_trip_data.csv")

  csvFile.write.json("/Users/saha/Documents/Big Data/Spark/data/bike-data/201508_trip_data.json")
}

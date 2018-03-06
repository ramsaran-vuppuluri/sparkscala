import org.apache.spark.sql.streaming.OutputMode

object Chapter21 extends App {
  val spark = LoadConfig(appName = "Chapter21").getSparkSession

  spark.conf.set("spark.sql.shuffle.partitions", 5)

  val jsonSchema = spark.read.json("/Users/saha/Documents/Spark/data/activity-data/part-00000-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json").schema

  val jsonStream = spark.readStream.schema(jsonSchema).option("maxFilePerTrigger", 1).json("/Users/saha/Documents/Spark/data/activity-data")

  val countByActivity = jsonStream.groupBy("gt").count

  countByActivity.writeStream.format("console").outputMode(OutputMode.Complete).start.awaitTermination(60000)


}
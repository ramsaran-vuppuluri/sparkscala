import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object Chapter22 extends App {
  val spark = LoadConfig(appName = "Chapter22").getSparkSession

  val toTimeStamp = udf((timeInNanoSeconds: Long) => new Timestamp(timeInNanoSeconds / 1000000))

  spark.conf.set("spark.sql.shuffle.partitions", 5)

  val jsonStatic = spark.read.json("/Users/saha/Documents/Spark/data/activity-data")

  val jsonStreaming = spark.readStream.schema(jsonStatic.schema).option("maxFilesPerTrigger", 10).json("/Users/saha/Documents/Spark/data/activity-data")

  val withEventTime = jsonStreaming.withColumn("event_time", toTimeStamp(col("Creation_Time")))

  withEventTime.withWatermark("event_time", "5 hours").groupBy(window(col("event_time"), "10 minutes", "5 minutes")).count.writeStream.format("memory").outputMode(OutputMode.Complete).queryName("watermarked_events_per_window").start.awaitTermination(120000)

  val waterMarkerEventsPerWindowDf = spark.sql("SELECT * FROM watermarked_events_per_window")

  waterMarkerEventsPerWindowDf.sort(col("window")).show(truncate = false)
  /*Output of above statement
  +---------------------------------------------+------+
  |[2015-02-21 19:35:00.0,2015-02-21 19:45:00.0]|35    |
  |[2015-02-21 19:40:00.0,2015-02-21 19:50:00.0]|35    |
  |[2015-02-23 05:10:00.0,2015-02-23 05:20:00.0]|11515 |
  |[2015-02-23 05:15:00.0,2015-02-23 05:25:00.0]|55686 |
  |[2015-02-23 05:20:00.0,2015-02-23 05:30:00.0]|99178 |
  |[2015-02-23 05:25:00.0,2015-02-23 05:35:00.0]|101286|
  |[2015-02-23 05:30:00.0,2015-02-23 05:40:00.0]|100443|
  |[2015-02-23 05:35:00.0,2015-02-23 05:45:00.0]|98969 |
  |[2015-02-23 05:40:00.0,2015-02-23 05:50:00.0]|88681 |
  |[2015-02-23 05:45:00.0,2015-02-23 05:55:00.0]|132708|
  |[2015-02-23 05:50:00.0,2015-02-23 06:00:00.0]|160775|
  |[2015-02-23 05:55:00.0,2015-02-23 06:05:00.0]|120218|
  |[2015-02-23 06:00:00.0,2015-02-23 06:10:00.0]|106232|
  |[2015-02-23 06:05:00.0,2015-02-23 06:15:00.0]|101780|
  |[2015-02-23 06:10:00.0,2015-02-23 06:20:00.0]|91382 |
  |[2015-02-23 06:15:00.0,2015-02-23 06:25:00.0]|92946 |
  |[2015-02-23 06:20:00.0,2015-02-23 06:30:00.0]|75181 |
  |[2015-02-23 06:25:00.0,2015-02-23 06:35:00.0]|29794 |
  |[2015-02-23 07:05:00.0,2015-02-23 07:15:00.0]|14805 |
  |[2015-02-23 07:10:00.0,2015-02-23 07:20:00.0]|58984 |
  +---------------------------------------------+------+
  only showing top 20 rows
   */

  waterMarkerEventsPerWindowDf.printSchema
  /*Output of above statement
  root
   |-- window: struct (nullable = true)
   |    |-- start: timestamp (nullable = true)
   |    |-- end: timestamp (nullable = true)
   |-- count: long (nullable = false)
   */

  //windowSlidingWindow

  private def windowSlidingWindow = {
    withEventTime.groupBy(window(col("event_time"), "10 minutes")).count.writeStream.outputMode(OutputMode.Complete).format("memory").queryName("evenets_per_window").start.awaitTermination(150000)

    val eventsPerWindowDf = spark.sql("SELECT * FROM evenets_per_window")

    eventsPerWindowDf.sort(col("window")).show(truncate = false)
    /*Output of above statement
  +---------------------------------------------+------+
  |window                                       |count |
  +---------------------------------------------+------+
  |[2015-02-21 19:40:00.0,2015-02-21 19:50:00.0]|35    |
  |[2015-02-23 05:10:00.0,2015-02-23 05:20:00.0]|11515 |
  |[2015-02-23 05:20:00.0,2015-02-23 05:30:00.0]|99178 |
  |[2015-02-23 05:30:00.0,2015-02-23 05:40:00.0]|100443|
  |[2015-02-23 05:40:00.0,2015-02-23 05:50:00.0]|88681 |
  |[2015-02-23 05:50:00.0,2015-02-23 06:00:00.0]|160775|
  |[2015-02-23 06:00:00.0,2015-02-23 06:10:00.0]|106232|
  |[2015-02-23 06:10:00.0,2015-02-23 06:20:00.0]|91382 |
  |[2015-02-23 06:20:00.0,2015-02-23 06:30:00.0]|75181 |
  |[2015-02-23 07:10:00.0,2015-02-23 07:20:00.0]|58984 |
  |[2015-02-23 07:20:00.0,2015-02-23 07:30:00.0]|106291|
  |[2015-02-23 07:30:00.0,2015-02-23 07:40:00.0]|100853|
  |[2015-02-23 07:40:00.0,2015-02-23 07:50:00.0]|97897 |
  |[2015-02-23 07:50:00.0,2015-02-23 08:00:00.0]|105160|
  |[2015-02-23 08:00:00.0,2015-02-23 08:10:00.0]|165556|
  |[2015-02-23 08:10:00.0,2015-02-23 08:20:00.0]|162075|
  |[2015-02-23 08:20:00.0,2015-02-23 08:30:00.0]|106075|
  |[2015-02-23 08:30:00.0,2015-02-23 08:40:00.0]|96480 |
  |[2015-02-23 08:40:00.0,2015-02-23 08:50:00.0]|167565|
  |[2015-02-23 08:50:00.0,2015-02-23 09:00:00.0]|193453|
  +---------------------------------------------+------+
  only showing top 20 rows
   */

    eventsPerWindowDf.printSchema
    /*Output of above statement
  root
   |-- window: struct (nullable = true)
   |    |-- start: timestamp (nullable = true)
   |    |-- end: timestamp (nullable = true)
   |-- count: long (nullable = false)
   */

    withEventTime.groupBy(window(col("event_time"), "10 minutes", "5 minutes")).count.writeStream.queryName("evenets_per_sliding_window").format("memory").outputMode(OutputMode.Complete).start.awaitTermination(150000)

    val eventsPerSlidingWindowDf = spark.sql("SELECT * FROM evenets_per_sliding_window")

    eventsPerSlidingWindowDf.sort(col("window")).show(truncate = false)
    /*Output of above statement
  +---------------------------------------------+------+
  |[2015-02-21 19:35:00.0,2015-02-21 19:45:00.0]|35    |
  |[2015-02-21 19:40:00.0,2015-02-21 19:50:00.0]|35    |
  |[2015-02-23 05:10:00.0,2015-02-23 05:20:00.0]|11515 |
  |[2015-02-23 05:15:00.0,2015-02-23 05:25:00.0]|55686 |
  |[2015-02-23 05:20:00.0,2015-02-23 05:30:00.0]|99178 |
  |[2015-02-23 05:25:00.0,2015-02-23 05:35:00.0]|101286|
  |[2015-02-23 05:30:00.0,2015-02-23 05:40:00.0]|100443|
  |[2015-02-23 05:35:00.0,2015-02-23 05:45:00.0]|98969 |
  |[2015-02-23 05:40:00.0,2015-02-23 05:50:00.0]|88681 |
  |[2015-02-23 05:45:00.0,2015-02-23 05:55:00.0]|132708|
  |[2015-02-23 05:50:00.0,2015-02-23 06:00:00.0]|160775|
  |[2015-02-23 05:55:00.0,2015-02-23 06:05:00.0]|120218|
  |[2015-02-23 06:00:00.0,2015-02-23 06:10:00.0]|106232|
  |[2015-02-23 06:05:00.0,2015-02-23 06:15:00.0]|101780|
  |[2015-02-23 06:10:00.0,2015-02-23 06:20:00.0]|91382 |
  |[2015-02-23 06:15:00.0,2015-02-23 06:25:00.0]|92946 |
  |[2015-02-23 06:20:00.0,2015-02-23 06:30:00.0]|75181 |
  |[2015-02-23 06:25:00.0,2015-02-23 06:35:00.0]|29794 |
  |[2015-02-23 07:05:00.0,2015-02-23 07:15:00.0]|14805 |
  |[2015-02-23 07:10:00.0,2015-02-23 07:20:00.0]|58984 |
  +---------------------------------------------+------+
  only showing top 20 rows
   */

    eventsPerSlidingWindowDf.printSchema
    /*Output of above statement
  root
   |-- window: struct (nullable = true)
   |    |-- start: timestamp (nullable = true)
   |    |-- end: timestamp (nullable = true)
   |-- count: long (nullable = false)
   */
  }
}
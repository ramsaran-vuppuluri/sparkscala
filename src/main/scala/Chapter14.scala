import org.apache.spark.util.LongAccumulator

object Chapter14 extends App {
  val spark = LoadConfig(appName = "Chapter14").getSparkSession

  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")

  val words = spark.sparkContext.parallelize(myCollection, 2)

  //broadcastVariable

  import spark.implicits._

  val flights = spark.read.parquet("/Users/saha/Documents/Big Data/Spark/Data/data/flight-data/parquet/2010-summary.parquet/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet").as[Flight]

  val longAccumulator = new LongAccumulator

  val evenAccumulator = new EvenAccumulator

  accumulatorOperations


  private def accumulatorOperations = {
    spark.sparkContext.register(longAccumulator)

    spark.sparkContext.register(evenAccumulator)

    println(longAccumulator.value)
    /*Output of above statement
  0
   */

    flights.foreach(flight => accumulatorChinaFun(flight))

    println(longAccumulator.value)
    /*Output of above statement
  953
   */

    flights.foreach(flight => accumulatorChinaFun(flight))

    println(longAccumulator.value)
    /*Output of above statement
  1906
   */

    println(evenAccumulator.value)

    flights.foreach(flight => evenAccumulator.add(flight.count))

    println(evenAccumulator.value)
  }

  def accumulatorChinaFun(flight: Flight) = {
    if (flight.DEST_COUNTRY_NAME.toUpperCase == "CHINA" || flight.ORIGIN_COUNTRY_NAME.toUpperCase == "CHINA") {
      longAccumulator.add(flight.count.toLong)
    }
  }

  private def broadcastVariable = {
    val supplementData = Map("Spark" -> 1000, "Definitive" -> 200, "Big" -> -300, "Simple" -> 100)

    val suppBroadCast = spark.sparkContext.broadcast(supplementData)

    println(suppBroadCast.value)
    /*Output of above statement
    Map(Spark -> 1000, Definitive -> 200, Big -> 300, Simple -> 100)
    */

    words.map(word => (word, suppBroadCast.value.getOrElse(word, 0))).sortBy(wordPair => wordPair._2).collect.foreach(println)
    /*Output of above statement
    (Big,-300)
    (The,0)
    (Guide,0)
    (:,0)
    (Data,0)
    (Processing,0)
    (Made,0)
    (Simple,100)
    (Definitive,200)
    (Spark,1000)
    */
  }
}
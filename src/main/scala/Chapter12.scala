

object Chapter12 extends App {
  val spark = LoadConfig(appName = "Chapter12").getSparkSession

  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")

  val words = spark.sparkContext.parallelize(myCollection, 2)

  words.name = "myWords"

  words.filter(word => startsWithS(word)).collect().foreach(println)
  /*Output of above statement
  Spark
  Simple
   */


  words.map(word => (word, word(0), startsWithS(word))).foreach(println)
  /*Output of above statement

  If we don't use collect it will process as two separate list as we declared the parallelizable factor as 2.

  (Spark,S,true)
  (The,T,false)
  (Definitive,D,false)
  (Guide,G,false)
  (:,:,false)
  18/01/13 16:44:15 INFO Executor: Finished task 0.0 in stage 1.0 (TID 2). 708 bytes result sent to driver
  18/01/13 16:44:15 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3, localhost, executor driver, partition 1, PROCESS_LOCAL, 4869 bytes)
  18/01/13 16:44:15 INFO Executor: Running task 1.0 in stage 1.0 (TID 3)
  18/01/13 16:44:15 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 8 ms on localhost (executor driver) (1/2)
  (Big,B,false)
  (Data,D,false)
  (Processing,P,false)
  (Made,M,false)
  (Simple,S,true)
   */

  words.map(word => (word, word(0), startsWithS(word))).collect.foreach(println)
  /*Output of above statement
  (Spark,S,true)
  (The,T,false)
  (Definitive,D,false)
  (Guide,G,false)
  (:,:,false)
  (Big,B,false)
  (Data,D,false)
  (Processing,P,false)
  (Made,M,false)
  (Simple,S,true)
   */

  words.flatMap(word => word.toSeq).take(5).foreach(println)
  /*Output of above statement
  S
  p
  a
  r
  k
   */

  words.sortBy(word => word.length * -1).collect.foreach(println)
  /*Output of above statement
  Definitive
  Processing
  Simple
  Spark
  Guide
  Data
  Made
  The
  Big
  :
   */

  words.sortBy(word => word.length).collect.foreach(println)
  /*Output of above statement
  :
  The
  Big
  Data
  Made
  Spark
  Guide
  Simple
  Definitive
  Processing
   */

  println(spark.sparkContext.parallelize(1 to 20).reduce(_ + _))
  /*Output of above statement
  210
   */

  println(words.reduce(findMaxLenghtWord))
  /*Output of above statement Reducer doesn't need collect to be explicitly specified
  Processing
   */

  println(words.count)
  /*Output of above statement
  10
   */


  /*
  countApproxDistinct

  There are two implementations of this, both based on streamlib’s implementation of “HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm.”

  In the first implementation, the argument we pass into the function is the relative accuracy. Smaller values create counters that require more space. The value must be greater than 0.000017.
   */

  println(words.countApprox(400, 0.95))
  /*Output of above statement
  (final: [10.000, 10.000])
   */

  /*
  With the other implementation you have a bit more control; you specify the relative accuracy based on two parameters: one for “regular” data and another for a sparse representation.

  The two arguments are p and sp where p is precision and sp is sparse precision. The relative accuracy is approximately 1.054 / sqrt(2P). Setting a nonzero (sp > p) triggers sparse representation of registers, which can reduce the memory consumption and increase accuracy when the cardinality is small. Both values are integers:
   */


  println(words.countApproxDistinct(4, 10))
  /*Output of above statement
  10
   */

  words.countByValue.foreach(println)
  /*Output of above statement
  (Definitive,1)
  (Simple,1)
  (Processing,1)
  (The,1)
  (Spark,1)
  (Made,1)
  (Guide,1)
  (Big,1)
  (:,1)
  (Data,1)
   */

  println(words.countByValueApprox(1000, 0.95))
  /*Output of above statement
  (final: Map(Definitive -> [1.000, 1.000], Simple -> [1.000, 1.000], Processing -> [1.000, 1.000], The -> [1.000, 1.000], Spark -> [1.000, 1.000], Made -> [1.000, 1.000], Guide -> [1.000, 1.000], Big -> [1.000, 1.000], : -> [1.000, 1.000], Data -> [1.000, 1.000]))
   */

  println(words.first)
  /*Output of above statement
  Spark
   */

  println(spark.sparkContext.parallelize(1 to 20).max)
  /*Output of above statement
  20
   */

  println(spark.sparkContext.parallelize(1 to 20).min)
  /*Output of above statement
  1
   */

  words.take(5).foreach(println)
  /*Output of above statement
  Spark
  The
  Definitive
  Guide
  :
   */

  words.takeOrdered(5).foreach(println)
  /*Output of above statement
  :
  Big
  Data
  Definitive
  Guide
   */

  words.top(5).foreach(println)
  /*Output of above statement
  The
  Spark
  Simple
  Processing
  Made
   */

  words.takeSample(withReplacement = true, num = 5, seed = 100L).foreach(println)
  /*Output of above statement
  Guide
  Spark
  Made
  Spark
  Processing
   */

  println(words.getStorageLevel)
  /*Output of above statement
  StorageLevel(1 replicas)
   */

  words.glom.collect.foreach(ele => ele.foreach(println))
  /*Output of above statement
  Spark
  The
  Definitive
  Guide
  :
  Big
  Data
  Processing
  Made
  Simple
   */

  def findMaxLenghtWord(leftWord: String, rightWord: String): String = {
    if (leftWord.length > rightWord.length) {
      leftWord
    } else {
      rightWord
    }
  }

  def startsWithS(stringIn: String): Boolean = {
    stringIn.startsWith("S")
  }
}
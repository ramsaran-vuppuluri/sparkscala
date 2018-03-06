import scala.util.Random

object Chapter13 extends App {
  val spark = LoadConfig(appName = "Chapter13").getSparkSession

  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")

  val words = spark.sparkContext.parallelize(myCollection, 2)

  //keyValueOperations

  //randomSampleSelection

  val chars = words.flatMap(word => word.toLowerCase.toSeq)

  val KVChars = chars.map(letter => (letter, 1))

  //keyValueAggregartionOperations

  val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct

  val keyedChars = distinctChars.map(c => (c, new Random().nextDouble))

  println(KVChars.join(keyedChars).count)
  /*Output of above statement
  51
   */
  println(KVChars.join(keyedChars, 10).count)
  /*Output of above statement
  51
   */

  val numRanger = spark.sparkContext.parallelize(0 to 9, 2)

  words.zip(numRanger).collect.foreach(println)
  /*Output of above statement this is same as cartesian product
  (Spark,0)
  (The,1)
  (Definitive,2)
  (Guide,3)
  (:,4)
  (Big,5)
  (Data,6)
  (Processing,7)
  (Made,8)
  (Simple,9)
   */

  println(words.coalesce(1).getNumPartitions)
  /*Output of above statement
  1
   */

  println(words.repartition(10).getNumPartitions)
  /*Output of above statement
  10
   */

  private def keyValueAggregartionOperations = {

    println(KVChars.countByKey())
    /*Output of above statement
  Map(e -> 7, s -> 4, n -> 2, t -> 3, u -> 1, f -> 1, a -> 4, m -> 2, i -> 7, v -> 1, b -> 1, g -> 3, l -> 1, p -> 3, c -> 1, h -> 1, r -> 2, : -> 1, k -> 1, o -> 1, d -> 4)
   */

    println(KVChars.countByKeyApprox(1000L, 0.95))
    /*Output of above statement
  (final: Map(e -> [7.000, 7.000], s -> [4.000, 4.000], n -> [2.000, 2.000], t -> [3.000, 3.000], u -> [1.000, 1.000], f -> [1.000, 1.000], a -> [4.000, 4.000], m -> [2.000, 2.000], i -> [7.000, 7.000], v -> [1.000, 1.000], b -> [1.000, 1.000], g -> [3.000, 3.000], l -> [1.000, 1.000], p -> [3.000, 3.000], c -> [1.000, 1.000], h -> [1.000, 1.000], r -> [2.000, 2.000], : -> [1.000, 1.000], k -> [1.000, 1.000], o -> [1.000, 1.000], d -> [4.000, 4.000]))
   */

    KVChars.groupByKey.map(row => (row._1, row._2.reduce(addFun))).collect.foreach(println)
    /*Output of above statement
    (d,4)
    (p,3)
    (t,3)
    (b,1)
    (h,1)
    (n,2)
    (f,1)
    (v,1)
    (:,1)
    (r,2)
    (l,1)
    (s,4)
    (e,7)
    (a,4)
    (k,1)
    (i,7)
    (u,1)
    (o,1)
    (g,3)
    (m,2)
    (c,1)
     */

    KVChars.reduceByKey(addFun).collect.foreach(println)
    /*Output of above statement
    (d,4)
    (p,3)
    (t,3)
    (b,1)
    (h,1)
    (n,2)
    (f,1)
    (v,1)
    (:,1)
    (r,2)
    (l,1)
    (s,4)
    (e,7)
    (a,4)
    (k,1)
    (i,7)
    (u,1)
    (o,1)
    (g,3)
    (m,2)
    (c,1)
     */

    val nums = spark.sparkContext.parallelize(1 to 30, 5)

    println(nums.aggregate(0)(math.max, addFun))
    /*Output of above statement
    90
     */

    println(nums.aggregate(1)(math.max, addFun))
    /*Output of above statement
    91
     */

    println(nums.treeAggregate(0)(math.max, addFun, 3))
    /*Output of above statement
    90
     */

    println(nums.treeAggregate(1)(math.max, addFun, 3))
    /*Output of above statement
    90
     */

    KVChars.foldByKey(0)(addFun).collect.foreach(println)
    /*Output of above statement
    (d,4)
    (p,3)
    (t,3)
    (b,1)
    (h,1)
    (n,2)
    (f,1)
    (v,1)
    (:,1)
    (r,2)
    (l,1)
    (s,4)
    (e,7)
    (a,4)
    (k,1)
    (i,7)
    (u,1)
    (o,1)
    (g,3)
    (m,2)
    (c,1)
     */
  }

  def addFun(x: Int, y: Int): Int = x + y

  private def randomSampleSelection = {
    val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct.collect

    val sampleMap = distinctChars.map(ch => (ch, new Random().nextDouble)).toMap

    words.map(word => (word.toLowerCase.toSeq(0), word)).sampleByKey(true, sampleMap, 6L).collect.foreach(println)
    /*Output of above statement
    Run 1:
    (s,Spark)
    (t,The)
    (d,Definitive)
    (g,Guide)

    Run 2:
    (d,Definitive)
    (g,Guide)
    (:,:)

    Run 3:
    (s,Spark)
    (t,The)
    (g,Guide)
    (:,:)
    */
  }

  private def keyValueOperations = {
    val keyWord = words.keyBy(word => word.toLowerCase.toSeq(0).toString)

    words.map(word => (word.toLowerCase, 1)).collect.foreach(println)
    /*Output of above statement
    (spark,1)
    (the,1)
    (definitive,1)
    (guide,1)
    (:,1)
    (big,1)
    (data,1)
    (processing,1)
    (made,1)
    (simple,1)
    */

    keyWord.collect.foreach(println)
    /*Output of above statement
    (s,Spark)
    (t,The)
    (d,Definitive)
    (g,Guide)
    (:,:)
    (b,Big)
    (d,Data)
    (p,Processing)
    (m,Made)
    (s,Simple)
    */

    keyWord.mapValues(word => word.toUpperCase).collect.foreach(println)
    /*Output of above statement
    (s,SPARK)
    (t,THE)
    (d,DEFINITIVE)
    (g,GUIDE)
    (:,:)
    (b,BIG)
    (d,DATA)
    (p,PROCESSING)
    (m,MADE)
    (s,SIMPLE)
    */

    keyWord.flatMapValues(word => word.toUpperCase).collect.foreach(println)
    /*Output of above statement
    (s,S)
    (s,P)
    (s,A)
    (s,R)
    (s,K)
    (t,T)
    (t,H)
    (t,E)
    (d,D)
    (d,E)
    (d,F)
    (d,I)
    (d,N)
    (d,I)
    (d,T)
    (d,I)
    (d,V)
    (d,E)
    (g,G)
    (g,U)
    (g,I)
    (g,D)
    (g,E)
    (:,:)
    (b,B)
    (b,I)
    (b,G)
    (d,D)
    (d,A)
    (d,T)
    (d,A)
    (p,P)
    (p,R)
    (p,O)
    (p,C)
    (p,E)
    (p,S)
    (p,S)
    (p,I)
    (p,N)
    (p,G)
    (m,M)
    (m,A)
    (m,D)
    (m,E)
    (s,S)
    (s,I)
    (s,M)
    (s,P)
    (s,L)
    (s,E)
    */

    keyWord.keys.collect.foreach(println)
    /*Output of above statement
    s
    t
    d
    g
    :
    b
    d
    p
    m
    s
    */

    keyWord.values.collect.foreach(println)
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

    println(keyWord.lookup("s"))
    /*Output of above statement
    WrappedArray(Spark, Simple)
    */
  }
}
final object Chapter8 extends App {
  val spark = LoadConfig(appName = "Chapter8").getSparkSession

  val persons = spark.createDataFrame(Seq((0, "Bill Chambers", 0, Seq(100)), (1, "Bill Chambers", 1, Seq(500, 250, 100)), (2, "Bill Chambers", 1, Seq(250, 100)))).toDF("id", "name", "graduate_program", "spark_status")

  val graduateProgram = spark.createDataFrame(Seq((0, "Masters", "School of Information", "UC Berkeley"), (2, "Masters", "EECS", "UC Berkeley"), (1, "Ph.D.", "EECS", "UC Berkeley"))).toDF("id", "degree", "department", "school")

  val sparkStatus = spark.createDataFrame(Seq((500, "Vice President"), (250, "PMC Member"), (100, "Contributor"))).toDF("id", "status")

  persons.createOrReplaceTempView("personsView")
  graduateProgram.createOrReplaceTempView("graduateProgramView")
  sparkStatus.createOrReplaceTempView("sparkStatusView")

  val joinExpression = persons.col("graduate_program") === graduateProgram.col("id")

  persons.join(graduateProgram, joinExpression).show()
  /*Output of above statement
  +---+-------------+----------------+---------------+---+-------+--------------------+-----------+
  | id|         name|graduate_program|   spark_status| id| degree|          department|     school|
  +---+-------------+----------------+---------------+---+-------+--------------------+-----------+
  |  0|Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
  |  1|Bill Chambers|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
  |  2|Bill Chambers|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
  +---+-------------+----------------+---------------+---+-------+--------------------+-----------+
   */

  persons.join(graduateProgram, Seq("id")).show()
  /*Output of above statement
  +---+-------------+----------------+---------------+-------+--------------------+-----------+
  | id|         name|graduate_program|   spark_status| degree|          department|     school|
  +---+-------------+----------------+---------------+-------+--------------------+-----------+
  |  0|Bill Chambers|               0|          [100]|Masters|School of Informa...|UC Berkeley|
  |  1|Bill Chambers|               1|[500, 250, 100]|  Ph.D.|                EECS|UC Berkeley|
  |  2|Bill Chambers|               1|     [250, 100]|Masters|                EECS|UC Berkeley|
  +---+-------------+----------------+---------------+-------+--------------------+-----------+
   */

  persons.join(graduateProgram, Seq("id"), JoinType.INNER.joingType()).show()
  /*Output of above statement
  +---+-------------+----------------+---------------+-------+--------------------+-----------+
  | id|         name|graduate_program|   spark_status| degree|          department|     school|
  +---+-------------+----------------+---------------+-------+--------------------+-----------+
  |  0|Bill Chambers|               0|          [100]|Masters|School of Informa...|UC Berkeley|
  |  1|Bill Chambers|               1|[500, 250, 100]|  Ph.D.|                EECS|UC Berkeley|
  |  2|Bill Chambers|               1|     [250, 100]|Masters|                EECS|UC Berkeley|
  +---+-------------+----------------+---------------+-------+--------------------+-----------+
   */

  persons.join(graduateProgram, joinExpression, JoinType.OUTER.joingType()).show
  /*Output of above statement
  +----+-------------+----------------+---------------+---+-------+--------------------+-----------+
  |  id|         name|graduate_program|   spark_status| id| degree|          department|     school|
  +----+-------------+----------------+---------------+---+-------+--------------------+-----------+
  |   1|Bill Chambers|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
  |   2|Bill Chambers|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
  |null|         null|            null|           null|  2|Masters|                EECS|UC Berkeley|
  |   0|Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
  +----+-------------+----------------+---------------+---+-------+--------------------+-----------+
   */

  persons.join(graduateProgram, joinExpression, JoinType.LEFT_OUTER.joingType()).show
  /*Output of above statement
  +---+-------------+----------------+---------------+---+-------+--------------------+-----------+
  | id|         name|graduate_program|   spark_status| id| degree|          department|     school|
  +---+-------------+----------------+---------------+---+-------+--------------------+-----------+
  |  0|Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
  |  1|Bill Chambers|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
  |  2|Bill Chambers|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
  +---+-------------+----------------+---------------+---+-------+--------------------+-----------+
   */

  persons.join(graduateProgram, joinExpression, JoinType.RIGHT_OUTER.joingType()).show
  /*Output of above statement
  +----+-------------+----------------+---------------+---+-------+--------------------+-----------+
  |  id|         name|graduate_program|   spark_status| id| degree|          department|     school|
  +----+-------------+----------------+---------------+---+-------+--------------------+-----------+
  |   0|Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
  |null|         null|            null|           null|  2|Masters|                EECS|UC Berkeley|
  |   2|Bill Chambers|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
  |   1|Bill Chambers|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
  +----+-------------+----------------+---------------+---+-------+--------------------+-----------+
   */

  persons.join(graduateProgram, joinExpression, JoinType.LEFT_SEMI.joingType).show
  /*Output of above statement
  +---+-------------+----------------+---------------+
  | id|         name|graduate_program|   spark_status|
  +---+-------------+----------------+---------------+
  |  0|Bill Chambers|               0|          [100]|
  |  1|Bill Chambers|               1|[500, 250, 100]|
  |  2|Bill Chambers|               1|     [250, 100]|
  +---+-------------+----------------+---------------+
   */

  graduateProgram.join(persons, joinExpression, JoinType.LEFT_SEMI.joingType).show
  /*Output of above statement
  +---+-------+--------------------+-----------+
  | id| degree|          department|     school|
  +---+-------+--------------------+-----------+
  |  0|Masters|School of Informa...|UC Berkeley|
  |  1|  Ph.D.|                EECS|UC Berkeley|
  +---+-------+--------------------+-----------+
   */

  val graduateProgram2 = graduateProgram.union(spark.createDataFrame(Seq((0, "Masters", "Duplicated Row", "Duplicated School"))).toDF("id", "degree", "department", "school"))

  graduateProgram2.createOrReplaceTempView("gradProgram2Table")

  graduateProgram2.join(persons, joinExpression, JoinType.LEFT_SEMI.joingType).show
  /*Output of above statement
  +---+-------+--------------------+-----------------+
  | id| degree|          department|           school|
  +---+-------+--------------------+-----------------+
  |  0|Masters|School of Informa...|      UC Berkeley|
  |  1|  Ph.D.|                EECS|      UC Berkeley|
  |  0|Masters|      Duplicated Row|Duplicated School|
  +---+-------+--------------------+-----------------+
   */

  graduateProgram.join(persons, joinExpression, JoinType.LEFT_ANTI.joingType).show
  /*Output of above statement
  +---+-------+----------+-----------+
  | id| degree|department|     school|
  +---+-------+----------+-----------+
  |  2|Masters|      EECS|UC Berkeley|
  +---+-------+----------+-----------+
   */

  graduateProgram.join(persons, joinExpression, JoinType.CROSS.joingType).show
  /*Output of above statement
  +---+-------+--------------------+-----------+---+-------------+----------------+---------------+
  | id| degree|          department|     school| id|         name|graduate_program|   spark_status|
  +---+-------+--------------------+-----------+---+-------------+----------------+---------------+
  |  0|Masters|School of Informa...|UC Berkeley|  0|Bill Chambers|               0|          [100]|
  |  1|  Ph.D.|                EECS|UC Berkeley|  2|Bill Chambers|               1|     [250, 100]|
  |  1|  Ph.D.|                EECS|UC Berkeley|  1|Bill Chambers|               1|[500, 250, 100]|
  +---+-------+--------------------+-----------+---+-------------+----------------+---------------+
   */

  graduateProgram.crossJoin(persons).show
  /*Output of above statement
  +---+-------+--------------------+-----------+---+-------------+----------------+---------------+
  | id| degree|          department|     school| id|         name|graduate_program|   spark_status|
  +---+-------+--------------------+-----------+---+-------------+----------------+---------------+
  |  0|Masters|School of Informa...|UC Berkeley|  0|Bill Chambers|               0|          [100]|
  |  0|Masters|School of Informa...|UC Berkeley|  1|Bill Chambers|               1|[500, 250, 100]|
  |  0|Masters|School of Informa...|UC Berkeley|  2|Bill Chambers|               1|     [250, 100]|
  |  2|Masters|                EECS|UC Berkeley|  0|Bill Chambers|               0|          [100]|
  |  2|Masters|                EECS|UC Berkeley|  1|Bill Chambers|               1|[500, 250, 100]|
  |  2|Masters|                EECS|UC Berkeley|  2|Bill Chambers|               1|     [250, 100]|
  |  1|  Ph.D.|                EECS|UC Berkeley|  0|Bill Chambers|               0|          [100]|
  |  1|  Ph.D.|                EECS|UC Berkeley|  1|Bill Chambers|               1|[500, 250, 100]|
  |  1|  Ph.D.|                EECS|UC Berkeley|  2|Bill Chambers|               1|     [250, 100]|
  +---+-------+--------------------+-----------+---+-------------+----------------+---------------+
   */

  val graduateProgramDuplicate = graduateProgram.withColumnRenamed("id", "graduate_program")

  val joinExpresionDup = graduateProgramDuplicate.col("graduate_program") === persons.col("graduate_program")

  persons.join(graduateProgramDuplicate, joinExpresionDup).select(persons.col("graduate_program"), graduateProgramDuplicate.col("graduate_program")).show
  /*Output of above statement
  +----------------+----------------+
  |graduate_program|graduate_program|
  +----------------+----------------+
  |               0|               0|
  |               1|               1|
  |               1|               1|
  +----------------+----------------+
   */

  persons.join(graduateProgramDuplicate, joinExpresionDup).select(persons.col("graduate_program"), graduateProgramDuplicate.col("graduate_program")).explain
  /*Output of above statement
  == Physical Plan ==
  *BroadcastHashJoin [graduate_program#11], [graduate_program#420], Inner, BuildRight
  :- LocalTableScan [graduate_program#11]
  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
     +- LocalTableScan [graduate_program#420]
   */

}
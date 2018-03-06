import org.apache.spark.sql.functions._

object Chapter6 extends App {
  val spark = LoadConfig(appName = "Chapter06").getSparkSession

  //processRetailDataByDay

  //processDateTime

  processUserDefinedFunctions

  private def processUserDefinedFunctions = {
    val powerUdf = udf(powerOf3(_: Double): Double)

    val udfExampleDf = spark.range(5).toDF("num")
    udfExampleDf.select(powerUdf(col("num"))).show(truncate = false)
    udfExampleDf.selectExpr("powerOf3(num)").show(truncate = false)
  }

  private def powerOf3(doubleValue: Double): Double = doubleValue * doubleValue * doubleValue

  private def processDateTime = {
    val TODAY: String = "today"
    val NOW: String = "now"
    val dateDf = spark.range(10).withColumn(TODAY, current_date).withColumn(NOW, current_timestamp)
    dateDf.createOrReplaceTempView("dateDfTable")
    dateDf.printSchema
    /*Output of above statement
  root
 |-- id: long (nullable = false)
 |-- today: date (nullable = false)
 |-- now: timestamp (nullable = false)
   */

    dateDf.show(2, false)
    /*Output of above statement
  +---+----------+-----------------------+
  |id |today     |now                    |
  +---+----------+-----------------------+
  |0  |2017-12-30|2017-12-30 23:15:38.543|
  |1  |2017-12-30|2017-12-30 23:15:38.543|
  +---+----------+-----------------------+
  only showing top 2 rows
   */

    dateDf.select(date_add(col(TODAY), 5), date_sub(col(TODAY), 5)).show(2, false)
    /*Output of above statement
  +------------------+------------------+
  |date_add(today, 5)|date_sub(today, 5)|
  +------------------+------------------+
  |2018-01-04        |2017-12-25        |
  |2018-01-04        |2017-12-25        |
  +------------------+------------------+
  only showing top 2 rows
   */

    dateDf.withColumn("weekAgo", date_sub(col(TODAY), 7)).select(datediff(col("weekAgo"), col(TODAY))).show(1)
    /*Output of above statement
  +------------------------+
  |datediff(weekAgo, today)|
  +------------------------+
  |                      -7|
  +------------------------+
  only showing top 1 row
   */
    dateDf.withColumn("startDate", to_date(lit("2016-01-01"))).withColumn("endDate", to_date(lit("2017-05-22"))).select(months_between(col("endDate"), col("startDate"))).show(1)
    /*Output of above statement
  |months_between(endDate, startDate)|
  +----------------------------------+
  |                       16.67741935|
  +----------------------------------+
  only showing top 1 row
   */

    dateDf.select(to_date(lit("2016-20-12")), to_date(lit("2016-02-12"))).show(1)
    /*Output of above statement
  +-------------------+-------------------+
  |to_date(2016-20-12)|to_date(2016-02-12)|
  +-------------------+-------------------+
  |               null|         2016-02-12|
  +-------------------+-------------------+
   */

    val simpleDateFormat = "yyyy-dd-MM"
    dateDf.select(to_date(lit("2016-20-12"), simpleDateFormat), to_date(lit("2016-02-12"), simpleDateFormat)).show(1)
    /*Output of above statement
  +----------------------------------+----------------------------------+
  |to_date('2016-20-12', 'yyyy-dd-MM'|to_date('2016-02-12', 'yyyy-dd-MM'|
  +----------------------------------+----------------------------------+
  |                        2016-12-20|                        2016-12-02|
  +----------------------------------+----------------------------------+
   */
  }


  private def processRetailDataByDay = {
    val retailDataByDayDf = spark.read.option(ProjectConstants.INFERE_SCHEMA, ProjectConstants.TRUE).option(ProjectConstants.CSV_HEADER, ProjectConstants.TRUE).csv("/Users/saha/Documents/Big Data/Spark/Data/data/retail-data/by-day/2010-12-01.csv");

    retailDataByDayDf.printSchema()
    /*Output of above statement
    root
    |-- InvoiceNo: string (nullable = true)
    |-- StockCode: string (nullable = true)
    |-- Description: string (nullable = true)
    |-- Quantity: string (nullable = true)
    |-- InvoiceDate: string (nullable = true)
    |-- UnitPrice: string (nullable = true)
    |-- CustomerID: string (nullable = true)
    |-- Country: string (nullable = true)
    */
    retailDataByDayDf.createGlobalTempView("retailDataByDayDfTable")

    retailDataByDayDf.select(lit(5), lit("five"), lit(5.0)).show(1)
    /*Output of above statement

    1st line is the column header
    +---+----+---+
    |  5|five|5.0|
    +---+----+---+
    |  5|five|5.0|
    +---+----+---+
    */

    retailDataByDayDf.where(col("InvoiceNo").eqNullSafe("536365")).select("InvoiceNo", "Description").show(5, false)

    /*Output of above statement

    +---------+-----------------------------------+
    |InvoiceNo|Description                        |
    +---------+-----------------------------------+
    |536365   |WHITE HANGING HEART T-LIGHT HOLDER |
    |536365   |WHITE METAL LANTERN                |
    |536365   |CREAM CUPID HEARTS COAT HANGER     |
    |536365   |KNITTED UNION FLAG HOT WATER BOTTLE|
    |536365   |RED WOOLLY HOTTIE WHITE HEART.     |
    +---------+-----------------------------------+
    */

    retailDataByDayDf.where("InvoiceNo=536365").show(5, false)

    val priceFilter = col("UnitPrice") > 600
    val descriptionFilter = col("Description").contains("POSTAGE")
    retailDataByDayDf.where(col("StockCode").isin("DOT")).where(priceFilter.or(descriptionFilter)).show(truncate = false) // This will not truncate  the output.
    /*Output of above statement
    +---------+---------+--------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|Description   |Quantity|InvoiceDate        |UnitPrice|CustomerID|Country       |
    +---------+---------+--------------+--------+-------------------+---------+----------+--------------+
    |536544   |DOT      |DOTCOM POSTAGE|1       |2010-12-01 14:32:00|569.77   |null      |United Kingdom|
    |536592   |DOT      |DOTCOM POSTAGE|1       |2010-12-01 17:06:00|607.49   |null      |United Kingdom|
    +---------+---------+--------------+--------+-------------------+---------+----------+--------------+
    */

    val DOTCodeFilter = col("StockCode").eqNullSafe("DOT")
    retailDataByDayDf.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descriptionFilter))).where("isExpensive").select("unitPrice", "isExpensive").show(5)
    /*Output of above statement
    root
    +---------+-----------+
    |unitPrice|isExpensive|
    +---------+-----------+
    |   569.77|       true|
    |   607.49|       true|
    +---------+-----------+
    */

    retailDataByDayDf.withColumn("isExpensive", not(col("UnitPrice").leq(250))).filter("isExpensive").select("Description", "UnitPrice").show(5)
    /*Output of above statement
    +--------------+---------+
    |   Description|UnitPrice|
    +--------------+---------+
    |DOTCOM POSTAGE|   569.77|
    |DOTCOM POSTAGE|   607.49|
    +--------------+---------+
    */

    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    retailDataByDayDf.select(col("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)
    /*Output of above statement
    +--------------+---------+
    |CustomerId|      realQuantity|
    +----------+------------------+
    |   17850.0|239.08999999999997|
    |   17850.0|          418.7156|
    +----------+------------------+
    */

    retailDataByDayDf.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)
    /*Output of above statement
    +-------+---------+
    |rounded|UnitPrice|
    +-------+---------+
    |    2.6|     2.55|
    |    3.4|     3.39|
    |    2.8|     2.75|
    |    3.4|     3.39|
    |    3.4|     3.39|
    +-------+---------+
    */
    retailDataByDayDf.select(round(lit("2.5")), bround(lit("2.5"))).show(2)
    /*Output of above statement
    +-------------+--------------+
    |round(2.5, 0)|bround(2.5, 0)|
    +-------------+--------------+
    |          3.0|           2.0|
    |          3.0|           2.0|
    +-------------+--------------+
    */

    retailDataByDayDf.select(corr("Quantity", "UnitPrice")).show
    /*Output of above statement, this is Pearson Correlation Coefficient for two columns to see if cheaper things are typically bought in greater quantities.
    +-------------------------+
    |corr(Quantity, UnitPrice)|
    +-------------------------+
    |     -0.04112314436835551|
    +-------------------------+
    */

    retailDataByDayDf.describe().show(truncate = false)
    /*Output of above statement
    +-------+-----------------+------------------+--------------------------------+------------------+-------------------+------------------+------------------+--------------+
    |summary|InvoiceNo        |StockCode         |Description                     |Quantity          |InvoiceDate        |UnitPrice         |CustomerID        |Country       |
    +-------+-----------------+------------------+--------------------------------+------------------+-------------------+------------------+------------------+--------------+
    |count  |3108             |3108              |3098                            |3108              |3108               |3108              |1968              |3108          |
    |mean   |536516.684944841 |27834.304044117645|null                            |8.627413127413128 |null               |4.151946589446603 |15661.388719512195|null          |
    |stddev |72.89447869788873|17407.897548583845|null                            |26.371821677029203|null               |15.638659854603892|1854.4496996893627|null          |
    |min    |536365           |10002             | 4 PURPLE FLOCK DINNER CANDLES  |-1                |2010-12-01 08:26:00|0.0               |12431.0           |Australia     |
    |max    |C536548          |POST              |ZINC WILLIE WINKIE  CANDLE STICK|96                |2010-12-01 17:35:00|9.95              |18229.0           |United Kingdom|
    +-------+-----------------+------------------+--------------------------------+------------------+-------------------+------------------+------------------+--------------+
    */

    retailDataByDayDf.select(initcap(col("Description"))).show(2, false)
    /*Output of above statement
    +----------------------------------+
    |initcap(Description)              |
    +----------------------------------+
    |White Hanging Heart T-light Holder|
    |White Metal Lantern               |
    +----------------------------------+
    */

    retailDataByDayDf.select(col("Description"), lower(col("Description")), upper(col("Description"))).show(2, false)
    /*Output of above statement
    +----------------------------------+----------------------------------+----------------------------------+
    |Description                       |lower(Description)                |upper(Description)                |
    +----------------------------------+----------------------------------+----------------------------------+
    |WHITE HANGING HEART T-LIGHT HOLDER|white hanging heart t-light holder|WHITE HANGING HEART T-LIGHT HOLDER|
    |WHITE METAL LANTERN               |white metal lantern               |WHITE METAL LANTERN               |
    +----------------------------------+----------------------------------+----------------------------------+
    */

    retailDataByDayDf.select(ltrim(lit("    HELLO    ")).as("ltrim"), rtrim(lit("    HELLO    ")).as("rtrim"), trim(lit
    ("    HELLO    ")).alias("trim"), lpad(lit("HELLOOOO  "), 3, "*").as("lpad"), rpad(lit("HELLOOOO  "), 3, " ").as
    ("rpad"))
      .show(2, false)
    /*Output of above statement
    +---------+---------+-----+----+----+
    |ltrim    |rtrim    |trim |lpad|rpad|
    +---------+---------+-----+----+----+
    |HELLO    |    HELLO|HELLO|HEL |HEL |
    |HELLO    |    HELLO|HELLO|HEL |HEL |
    +---------+---------+-----+----+----+
    */

    val simpleColors = Seq("black", "white", "red", "green", "blue")
    var regExString: String = simpleColors.map(_.toUpperCase).mkString("|")
    // the | signifies OR in regular expression syntax
    print(regExString)
    /*Output of above statement
    BLACK|WHITE|RED|GREEN|BLUE
    */

    retailDataByDayDf.select(regexp_replace(col("Description"), regExString, "COLOR").as("color_clean"), col("Description")).show(2)
    /*Output of above statement
    +--------------------+--------------------+
    |         color_clean|         Description|
    +--------------------+--------------------+
    |COLOR HANGING HEA...|WHITE HANGING HEA...|
    | COLOR METAL LANTERN| WHITE METAL LANTERN|
    +--------------------+--------------------+
    */

    retailDataByDayDf.select(translate(col("Description"), "LEET", "1337"), col("Description")).show(2)

    regExString = simpleColors.map(_.toUpperCase()).mkString("(", "|", ")")
    print(regExString)
    /*Output of above statement
    (BLACK|WHITE|RED|GREEN|BLUE)
    */

    retailDataByDayDf.select(regexp_extract(col("Description"), regExString, 1).as("color_clean"), col("Description")).show(2)
    /*Output of above statement
    +-----------+--------------------+
    |color_clean|         Description|
    +-----------+--------------------+
    |      WHITE|WHITE HANGING HEA...|
    |      WHITE| WHITE METAL LANTERN|
    +-----------+--------------------+
    */

    val containsBlack = col("Description").contains("BLACK")
    val containsWhite = col("Description").contains("WHITE")

    retailDataByDayDf.withColumn("hasSimpleColor", containsBlack.or(containsWhite)).where("hasSimpleColor").select(
      "Description").show(3, false)
    /*Output of above statement
    +----------------------------------+
    |Description                       |
    +----------------------------------+
    |WHITE HANGING HEART T-LIGHT HOLDER|
    |WHITE METAL LANTERN               |
    |RED WOOLLY HOTTIE WHITE HEART.    |
    +----------------------------------+
    */

    val selectedColumns = simpleColors.map(color => col("Description").contains(color.toUpperCase()).alias(s"is_$color")) :+ expr("*")
    print(selectedColumns.isInstanceOf)

    retailDataByDayDf.select(selectedColumns: _*).where(col("is_white").or(col("is_red"))).select("Description").show(3, false)
    /*Output of above statement
    +----------------------------------+
    |Description                       |
    +----------------------------------+
    |WHITE HANGING HEART T-LIGHT HOLDER|
    |WHITE METAL LANTERN               |
    |RED WOOLLY HOTTIE WHITE HEART.    |
    +----------------------------------+
    */

    retailDataByDayDf.select(coalesce(col("Description"), col("CustomerId"))).show(2)
    /*Output of above statement
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
    |   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
     */

    retailDataByDayDf.na.drop("any").show(2)
    /*Output of above statement
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
    |   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
     */

    retailDataByDayDf.na.drop("all").show(2)
    /*Output of above statement
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
    |   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
     */

    retailDataByDayDf.na.drop("any", Seq("StockCode", "InvoiceNo")).show(2)
    /*Output of above statement
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
    |   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
     */

    retailDataByDayDf.na.fill("All Null values become this string").show(2)
    /*Output of above statement
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
    |   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
     */

    retailDataByDayDf.na.fill(5, Seq("StockCode", "InvoiceNo")).show(2)
    /*Output of above statement
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
    |   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
     */

    val fillColByValues = Map("StockCode" -> 5, "Description" -> "No Value")
    retailDataByDayDf.na.fill(fillColByValues).show(2)
    /*Output of above statement
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
    |   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
     */

    retailDataByDayDf.na.replace("Description", Map("" -> "Unknown")).show(2)
    /*Output of above statement
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
    |   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    */

    retailDataByDayDf.selectExpr("(Description,InvoiceNo) as complex", "*").show(2, false)
    /*Output of above statement
    +-------------------------------------------+---------+---------+----------------------------------+--------+-------------------+---------+----------+--------------+
    |complex                                    |InvoiceNo|StockCode|Description                       |Quantity|InvoiceDate        |UnitPrice|CustomerID|Country       |
    +-------------------------------------------+---------+---------+----------------------------------+--------+-------------------+---------+----------+--------------+
    |[WHITE HANGING HEART T-LIGHT HOLDER,536365]|536365   |85123A   |WHITE HANGING HEART T-LIGHT HOLDER|6       |2010-12-01 08:26:00|2.55     |17850.0   |United Kingdom|
    |[WHITE METAL LANTERN,536365]               |536365   |71053    |WHITE METAL LANTERN               |6       |2010-12-01 08:26:00|3.39     |17850.0   |United Kingdom|
    +-------------------------------------------+---------+---------+----------------------------------+--------+-------------------+---------+----------+--------------+
     */

    val complexDf = retailDataByDayDf.select(struct(col("Description"), col("InvoiceNo")).alias("complex"))
    complexDf.createOrReplaceTempView("complexDfTable")

    complexDf.printSchema
    /*Output of above statement
    root
     |-- complex: struct (nullable = false)
     |    |-- Description: string (nullable = true)
     |    |-- InvoiceNo: string (nullable = true)
     */

    complexDf.select("complex.Description").show(2, false)
    /*Output of above statement
    +----------------------------------+
    |Description                       |
    +----------------------------------+
    |WHITE HANGING HEART T-LIGHT HOLDER|
    |WHITE METAL LANTERN               |
    +----------------------------------+
     */
    complexDf.select(col("complex").getField("Description")).show(2, false)
    /*Output of above statement
    +----------------------------------+
    |complex.Description               |
    +----------------------------------+
    |WHITE HANGING HEART T-LIGHT HOLDER|
    |WHITE METAL LANTERN               |
    +----------------------------------+
     */
    complexDf.select("complex.*").show(2, false)
    /*Output of above statement
    +----------------------------------+---------+
    |Description                       |InvoiceNo|
    +----------------------------------+---------+
    |WHITE HANGING HEART T-LIGHT HOLDER|536365   |
    |WHITE METAL LANTERN               |536365   |
    +----------------------------------+---------+
     */
    complexDf.select(col("complex")).show(2, false)
    /*Output of above statement
    +-------------------------------------------+
    |complex                                    |
    +-------------------------------------------+
    |[WHITE HANGING HEART T-LIGHT HOLDER,536365]|
    |[WHITE METAL LANTERN,536365]               |
    +-------------------------------------------+
     */

    retailDataByDayDf.select(split(col("Description"), " ")).show(2, false)
    /*Output of above statement
    +----------------------------------------+
    |split(Description,  )                   |
    +----------------------------------------+
    |[WHITE, HANGING, HEART, T-LIGHT, HOLDER]|
    |[WHITE, METAL, LANTERN]                 |
    +----------------------------------------+
     */

    retailDataByDayDf.select(split(col("Description"), " ").as("array_col")).selectExpr("array_col[0]").show(2, false)
    /*Output of above statement
    +------------+
    |array_col[0]|
    +------------+
    |WHITE       |
    |WHITE       |
    +------------+
     */

    retailDataByDayDf.select(size(split(col("Description"), " "))).show(2, false)
    /*Output of above statement
    +---------------------------+
    |size(split(Description,  ))|
    +---------------------------+
    |5                          |
    |3                          |
    +---------------------------+
     */

    retailDataByDayDf.select(array_contains(split(col("Description"), " "), "WHITE")).show(2, false)
    /*Output of above statement
    +--------------------------------------------+
    |array_contains(split(Description,  ), WHITE)|
    +--------------------------------------------+
    |true                                        |
    |true                                        |
    +--------------------------------------------+
     */

    retailDataByDayDf.withColumn("splitted", split(col("Description"), " ")).withColumn("exploded", explode(col("splitted"))).select(col("splitted"), col("InvoiceNo"), col("exploded")).show(2, false)
    /*Output of above statement
    +----------------------------------------+---------+--------+
    |splitted                                |InvoiceNo|exploded|
    +----------------------------------------+---------+--------+
    |[WHITE, HANGING, HEART, T-LIGHT, HOLDER]|536365   |WHITE   |
    |[WHITE, HANGING, HEART, T-LIGHT, HOLDER]|536365   |HANGING |
    +----------------------------------------+---------+--------+
     */

    retailDataByDayDf.select(map(col("Description"), col("InvoiceNo")).as("complex_map")).show(2, false)
    /*Output of above statement
    +-------------------------------------------------+
    |complex_map                                      |
    +-------------------------------------------------+
    |Map(WHITE HANGING HEART T-LIGHT HOLDER -> 536365)|
    |Map(WHITE METAL LANTERN -> 536365)               |
    +-------------------------------------------------+
     */

    retailDataByDayDf.select(map(col("Description"), col("InvoiceNo")).as("complex_map")).selectExpr("complex_map['WHITE METAL LANTERN']").show(2, truncate = false)
    /*Output of above statement
    +--------------------------------+
    |complex_map[WHITE METAL LANTERN]|
    +--------------------------------+
    |null                            |
    |536365                          |
    +--------------------------------+
     */

    retailDataByDayDf.select(map(col("Description"), col("InvoiceNo")).as("complex_map")).selectExpr("complex_map['WHITE METAL LANTERN']").show(2, false)
    /*Output of above statement
    +--------------------------------+
    |complex_map[WHITE METAL LANTERN]|
    +--------------------------------+
    |null                            |
    |536365                          |
    +--------------------------------+
     */

    retailDataByDayDf.select(map(col("Description"), col("InvoiceNo")).as("complex_map")).selectExpr("explode(complex_map)").show(2, false)
    /*Output of above statement
    +----------------------------------+------+
    |key                               |value |
    +----------------------------------+------+
    |WHITE HANGING HEART T-LIGHT HOLDER|536365|
    |WHITE METAL LANTERN               |536365|
    +----------------------------------+------+
     */

    retailDataByDayDf.select(explode(map(col("Description"), col("InvoiceNo")).as("complex_map"))).show(2, false)
    /*Output of above statement
    +----------------------------------+------+
    |key                               |value |
    +----------------------------------+------+
    |WHITE HANGING HEART T-LIGHT HOLDER|536365|
    |WHITE METAL LANTERN               |536365|
    +----------------------------------+------+
     */
  }
}
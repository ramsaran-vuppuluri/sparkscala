import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

final object Chapter7 extends App {
  val RETAIL_DATA_ALL_DF_TABLE = "retailDataAllDfTable"
  val INVOICE_NO = "InvoiceNo"
  val STOCK_CODE = "StockCode"
  val DESCRIPTION = "Description"
  val QUANTITY = "Quantity"
  val INVOICE_DATE = "InvoiceDate"
  val UNIT_PRICE = "UnitPrice"
  val CUSTOMER_ID = "CustomerID"
  val COUNTRY = "Country"

  val spark = LoadConfig(appName = "Chapter7").getSparkSession

  val retailDataAllDf = spark.read.option(ProjectConstants.INFERE_SCHEMA, true).option(ProjectConstants.CSV_HEADER, true).csv("/Users/saha/Documents/Big Data/Spark/Data/data/retail-data/all/online-retail-dataset.csv").coalesce(5)

  retailDataAllDf.cache

  retailDataAllDf.createOrReplaceTempView(RETAIL_DATA_ALL_DF_TABLE)

  //mathFunctions(retailDataAllDf)

  //statFunctions(retailDataAllDf)

  //groupFunctions(retailDataAllDf)

  //windowFunctions

  rollUpCubeFunctions

  private def rollUpCubeFunctions = {
    spark.sql("SELECT CustomerID, StockCode, SUM(Quantity) FROM retailDataAllDfTable GROUP BY CustomerID, StockCode GROUPING SETS(CustomerID, StockCode) ORDER BY CustomerID DESC, StockCode DESC").show(truncate = false)
    /*Output of above statement
    +----------+---------+-----------------------------+
    |CustomerID|StockCode|sum(CAST(Quantity AS DOUBLE))|
    +----------+---------+-----------------------------+
    |18287     |null     |1586.0                       |
    |18283     |null     |1397.0                       |
    |18282     |null     |98.0                         |
    |18281     |null     |54.0                         |
    |18280     |null     |45.0                         |
    |18278     |null     |66.0                         |
    |18277     |null     |67.0                         |
    |18276     |null     |184.0                        |
    |18274     |null     |0.0                          |
    |18273     |null     |80.0                         |
    |18272     |null     |2044.0                       |
    |18270     |null     |97.0                         |
    |18269     |null     |70.0                         |
    |18268     |null     |0.0                          |
    |18265     |null     |311.0                        |
    |18263     |null     |1467.0                       |
    |18262     |null     |182.0                        |
    |18261     |null     |146.0                        |
    |18260     |null     |1469.0                       |
    |18259     |null     |714.0                        |
    +----------+---------+-----------------------------+
    only showing top 20 rows
     */

    retailDataAllDf.rollup(col(INVOICE_DATE), col(COUNTRY)).agg(sum(QUANTITY)).selectExpr(INVOICE_DATE, COUNTRY, "`sum(Quantity)` AS SUM_QUANTITY").orderBy(INVOICE_DATE).show(truncate = false)
    /*Output of above statement
    +---------------+--------------+------------+
    |InvoiceDate    |Country       |SUM_QUANTITY|
    +---------------+--------------+------------+
    |null           |null          |5176450.0   |
    |1/10/2011 10:04|United Kingdom|-29.0       |
    |1/10/2011 10:04|null          |-29.0       |
    |1/10/2011 10:07|null          |-4.0        |
    |1/10/2011 10:07|EIRE          |-4.0        |
    |1/10/2011 10:08|United Kingdom|-14.0       |
    |1/10/2011 10:08|null          |-14.0       |
    |1/10/2011 10:32|null          |260.0       |
    |1/10/2011 10:32|United Kingdom|260.0       |
    |1/10/2011 10:35|null          |408.0       |
    |1/10/2011 10:35|Germany       |392.0       |
    |1/10/2011 10:35|United Kingdom|16.0        |
    |1/10/2011 10:36|United Kingdom|-2600.0     |
    |1/10/2011 10:36|null          |-2600.0     |
    |1/10/2011 10:44|United Kingdom|40.0        |
    |1/10/2011 10:44|null          |40.0        |
    |1/10/2011 10:58|null          |83.0        |
    |1/10/2011 10:58|United Kingdom|83.0        |
    |1/10/2011 11:09|null          |249.0       |
    |1/10/2011 11:09|United Kingdom|249.0       |
    +---------------+--------------+------------+
    only showing top 20 rows
     */

    retailDataAllDf.cube(col(INVOICE_DATE), col(COUNTRY)).agg(sum(QUANTITY)).selectExpr(INVOICE_DATE, COUNTRY, "`sum(Quantity)` AS SUM_QUANTITY").orderBy(INVOICE_DATE).show(truncate = false)
    /*Output of above statement
    +-----------+--------------------+------------+
    |InvoiceDate|Country             |SUM_QUANTITY|
    +-----------+--------------------+------------+
    |null       |Japan               |25218.0     |
    |null       |Portugal            |16180.0     |
    |null       |RSA                 |352.0       |
    |null       |null                |5176450.0   |
    |null       |Australia           |83653.0     |
    |null       |Germany             |117448.0    |
    |null       |Unspecified         |3300.0      |
    |null       |Finland             |10666.0     |
    |null       |USA                 |1034.0      |
    |null       |Singapore           |5234.0      |
    |null       |United Arab Emirates|982.0       |
    |null       |Cyprus              |6317.0      |
    |null       |Lebanon             |386.0       |
    |null       |Hong Kong           |4769.0      |
    |null       |Channel Islands     |9479.0      |
    |null       |European Community  |497.0       |
    |null       |Norway              |19247.0     |
    |null       |Spain               |26824.0     |
    |null       |Denmark             |8188.0      |
    |null       |Czech Republic      |592.0       |
    +-----------+--------------------+------------+
    only showing top 20 rows
     */

    retailDataAllDf.cube(col(INVOICE_DATE), col(COUNTRY)).agg(grouping_id(), sum(QUANTITY)).selectExpr(INVOICE_DATE, COUNTRY, "`sum(Quantity)` AS SUM_QUANTITY").orderBy(expr("grouping_id()").desc).show(truncate = false)
    /*Output of above statement
    +-----------+--------------------+------------+
    |InvoiceDate|Country             |SUM_QUANTITY|
    +-----------+--------------------+------------+
    |null       |null                |5176450.0   |
    |null       |Japan               |25218.0     |
    |null       |Hong Kong           |4769.0      |
    |null       |Portugal            |16180.0     |
    |null       |Channel Islands     |9479.0      |
    |null       |Cyprus              |6317.0      |
    |null       |Germany             |117448.0    |
    |null       |Australia           |83653.0     |
    |null       |Czech Republic      |592.0       |
    |null       |USA                 |1034.0      |
    |null       |United Arab Emirates|982.0       |
    |null       |Lebanon             |386.0       |
    |null       |Finland             |10666.0     |
    |null       |Unspecified         |3300.0      |
    |null       |RSA                 |352.0       |
    |null       |European Community  |497.0       |
    |null       |Norway              |19247.0     |
    |null       |Denmark             |8188.0      |
    |null       |Spain               |26824.0     |
    |null       |Singapore           |5234.0      |
    +-----------+--------------------+------------+
    only showing top 20 rows
     */
  }

  private def windowFunctions = {
    retailDataAllDf.withColumn("date", to_date(col(INVOICE_DATE), "MM/d/yyyy H:mm"))

    retailDataAllDf.createOrReplaceTempView("retailDataAllDfWithDate")

    val windowSpec = Window.partitionBy(CUSTOMER_ID, "date").orderBy(col(QUANTITY).desc).rangeBetween(Window.unboundedPreceding, Window.currentRow)

    val maxPurchaseQuantity = max(col(QUANTITY)).over(windowSpec)

    val denseRankQuantity = dense_rank.over(windowSpec)

    val rankQuantity = rank.over(windowSpec)

    retailDataAllDf.where(col(CUSTOMER_ID) =!= null).select(col(CUSTOMER_ID), col(QUANTITY), col("date"), maxPurchaseQuantity.as("Max Purchase quantity"), rankQuantity.as("Rank Quantity"), denseRankQuantity.as("Dense Rank Quantity")).show(truncate = false)
  }


  private def groupFunctions(retailDataAllDf: DataFrame) = {
    retailDataAllDf.select(collect_list(COUNTRY), collect_set(COUNTRY)).show
    /*Output of above statement
    +---------------------+--------------------+
    |collect_list(Country)|collect_set(Country)|
    +---------------------+--------------------+
    | [United Kingdom, ...|[Portugal, Italy,...|
    +---------------------+--------------------+
    */

    retailDataAllDf.groupBy(INVOICE_NO, CUSTOMER_ID).count.show(10, false)
    /*Output of above statement
    +---------+----------+-----+
    |InvoiceNo|CustomerID|count|
    +---------+----------+-----+
    |536395   |13767     |14   |
    |536609   |17850     |16   |
    |536785   |15061     |6    |
    |537205   |13034     |11   |
    |537829   |15498     |19   |
    |538513   |15454     |25   |
    |C538642  |12476     |2    |
    |538808   |14057     |43   |
    |538830   |14298     |5    |
    |539374   |14769     |22   |
    +---------+----------+-----+
    only showing top 10 rows
    */

    retailDataAllDf.groupBy(INVOICE_NO).agg(count(QUANTITY).as("quantity"), expr("count(Quantity)")).show
    /*Output of above statement
    +---------+--------+---------------+
    |InvoiceNo|quantity|count(Quantity)|
    +---------+--------+---------------+
    |   536596|       6|              6|
    |   536938|      14|             14|
    |   537252|       1|              1|
    |   537691|      20|             20|
    |   538041|       1|              1|
    |   538184|      26|             26|
    |   538517|      53|             53|
    |   538879|      19|             19|
    |   539275|       6|              6|
    |   539630|      12|             12|
    |   540499|      24|             24|
    |   540540|      22|             22|
    |  C540850|       1|              1|
    |   540976|      48|             48|
    |   541432|       4|              4|
    |   541518|     101|            101|
    |   541783|      35|             35|
    |   542026|       9|              9|
    |   542375|       6|              6|
    |  C542604|       8|              8|
    +---------+--------+---------------+
    only showing top 20 rows
    */

    retailDataAllDf.groupBy(INVOICE_NO).agg(avg(QUANTITY), stddev_pop(QUANTITY)).show
    /*Output of above statement
    +---------+------------------+--------------------+
    |InvoiceNo|     avg(Quantity)|stddev_pop(Quantity)|
    +---------+------------------+--------------------+
    |   536596|               1.5|  1.1180339887498947|
    |   536938|33.142857142857146|  20.698023172885524|
    |   537252|              31.0|                 0.0|
    |   537691|              8.15|   5.597097462078001|
    |   538041|              30.0|                 0.0|
    |   538184|12.076923076923077|   8.142590198943392|
    |   538517|3.0377358490566038|  2.3946659604837897|
    |   538879|21.157894736842106|  11.811070444356483|
    |   539275|              26.0|  12.806248474865697|
    |   539630|20.333333333333332|  10.225241100118645|
    |   540499|              3.75|  2.6653642652865788|
    |   540540|2.1363636363636362|  1.0572457590557278|
    |  C540850|              -1.0|                 0.0|
    |   540976|10.520833333333334|   6.496760677872902|
    |   541432|             12.25|  10.825317547305483|
    |   541518| 23.10891089108911|  20.550782784878713|
    |   541783|11.314285714285715|   8.467657556242811|
    |   542026| 7.666666666666667|   4.853406592853679|
    |   542375|               8.0|  3.4641016151377544|
    |  C542604|              -8.0|  15.173990905493518|
    +---------+------------------+--------------------+
    only showing top 20 rows
    */

    retailDataAllDf.groupBy(INVOICE_NO).agg("Quantity" -> "avg", "Quantity" -> "stddev_pop").show
    /*Output of above statement
    +---------+------------------+--------------------+
    |InvoiceNo|     avg(Quantity)|stddev_pop(Quantity)|
    +---------+------------------+--------------------+
    |   536596|               1.5|  1.1180339887498947|
    |   536938|33.142857142857146|  20.698023172885524|
    |   537252|              31.0|                 0.0|
    |   537691|              8.15|   5.597097462078001|
    |   538041|              30.0|                 0.0|
    |   538184|12.076923076923077|   8.142590198943392|
    |   538517|3.0377358490566038|  2.3946659604837897|
    |   538879|21.157894736842106|  11.811070444356483|
    |   539275|              26.0|  12.806248474865697|
    |   539630|20.333333333333332|  10.225241100118645|
    |   540499|              3.75|  2.6653642652865788|
    |   540540|2.1363636363636362|  1.0572457590557278|
    |  C540850|              -1.0|                 0.0|
    |   540976|10.520833333333334|   6.496760677872902|
    |   541432|             12.25|  10.825317547305483|
    |   541518| 23.10891089108911|  20.550782784878713|
    |   541783|11.314285714285715|   8.467657556242811|
    |   542026| 7.666666666666667|   4.853406592853679|
    |   542375|               8.0|  3.4641016151377544|
    |  C542604|              -8.0|  15.173990905493518|
    +---------+------------------+--------------------+
    only showing top 20 rows
    */
  }

  private def statFunctions(retailDataAllDf: DataFrame) = {
    /*
    Single column stat functions:

    The variance is the average of the squared differences from the mean, and the standard deviation is the square root of the variance.

    Skewness and kurtosis are both measurements of extreme points in your data. Skewness measures the asymmetry the values in your data around the mean, whereas kurtosis is a measure of the tail of data. These are both relevant specifically when modeling your data as a probability distribution of a random variable.
    */
    retailDataAllDf.select(var_pop(QUANTITY), var_samp(QUANTITY), stddev_pop(QUANTITY), stddev_samp(QUANTITY), skewness(QUANTITY), kurtosis(QUANTITY)).show
    /*Output of above statement
    +-----------------+------------------+--------------------+---------------------+------------------+------------------+
    |var_pop(Quantity)|var_samp(Quantity)|stddev_pop(Quantity)|stddev_samp(Quantity)|skewness(Quantity)|kurtosis(Quantity)|
    +-----------------+------------------+--------------------+---------------------+------------------+------------------+
    |47559.30364660879| 47559.39140929848|  218.08095663447733|   218.08115785023355|-0.264075576105298|119768.05495534067|
    +-----------------+------------------+--------------------+---------------------+------------------+------------------+
    */

    /*
    Multiple column stat functions:
    Correlation measures the Pearson correlation coefficient, which is scaled between –1 and +1. The covariance is scaled according to the inputs in the data.

    Like the var function, covariance can be calculated either as the sample covariance or the population covariance. Therefore it can be important to specify which formula you want to use. Correlation has no notion of this and therefore does not have calculations for population or sample.
    */
    retailDataAllDf.select(corr(INVOICE_NO, QUANTITY), covar_samp(INVOICE_NO, QUANTITY), covar_pop(INVOICE_NO, QUANTITY), covar_pop(QUANTITY, INVOICE_NO)).show
    /*Output of above statement
    +-------------------------+-------------------------------+------------------------------+------------------------------+
    |corr(InvoiceNo, Quantity)|covar_samp(InvoiceNo, Quantity)|covar_pop(InvoiceNo, Quantity)|covar_pop(Quantity, InvoiceNo)|
    +-------------------------+-------------------------------+------------------------------+------------------------------+
    |     4.912186085636837E-4|             1052.7280543912716|            1052.7260778751674|            1052.7260778751527|
    +-------------------------+-------------------------------+------------------------------+------------------------------+
    */
  }


  private def mathFunctions(retailDataAllDf: DataFrame) = {
    /*
    Often, we find ourselves working with large datasets and the exact distinct count is irrelevant. In fact, getting the distinct count is an expensive operation, and for large datasets it might take a long time to calculate the exact result. There are times when an approximation to a certain degree of accuracy will work just fine, and for that, you can use the approx_count_distinct function:

    You will notice that approx_count_distinct took another parameter with which you can specify the maximum estimation error allowed. In this case, we specified a rather large error and thus receive an answer that is quite far off but does complete more quickly than countDistinct. You will see much greater gains with larger datasets.
    */
    retailDataAllDf.select(approx_count_distinct(STOCK_CODE, 0.1)).show
    /*Output of above statement
    +--------------------------------+
    |approx_count_distinct(StockCode)|
    +--------------------------------+
    |                            3364|
    +--------------------------------+
    */

    retailDataAllDf.select(approx_count_distinct(STOCK_CODE, 0.39)).show
    /*Output of above statement
    +--------------------------------+
    |approx_count_distinct(StockCode)|
    +--------------------------------+
    |                            2448|
    +--------------------------------+
    */

    retailDataAllDf.select(first(STOCK_CODE), last(STOCK_CODE)).show

    /*Output of above statement
    +-----------------------+----------------------+
    |first(StockCode, false)|last(StockCode, false)|
    print(retailDataAllDf.count)
    /*Output of above statement
    541909
    */

    retailDataAllDf.select(count(STOCK_CODE)).show
    /*Output of above statement
    +----------------+
    |count(StockCode)|
    +----------------+
    |          541909|
    +----------------+
    */

    retailDataAllDf.select(countDistinct(STOCK_CODE)).show
    /*Output of above statement
    +-------------------------+
    |count(DISTINCT StockCode)|
    +-------------------------+
    |                     4070|
    +-------------------------+
    */
    +-----------------------+----------------------+
    |                 85123A|                 22138|
    +-----------------------+----------------------+
    */

    retailDataAllDf.select(min(QUANTITY), max(QUANTITY)).show
    /*Output of above statement
    +-------------+-------------+
    |min(Quantity)|max(Quantity)|
    +-------------+-------------+
    |           -1|          992|
    +-------------+-------------+
    */

    retailDataAllDf.select(sum(QUANTITY)).show
    /*Output of above statement
    +-------------+
    |sum(Quantity)|
    +-------------+
    |    5176450.0|
    +-------------+
    */

    retailDataAllDf.select(sumDistinct(QUANTITY)).show
    /*Output of above statement
    +----------------------+
    |sum(DISTINCT Quantity)|
    +----------------------+
    |               29310.0|
    +----------------------+
    */

    retailDataAllDf.select(count(QUANTITY).as("total_transactions"), sum(QUANTITY).as("total_purchases"), avg(QUANTITY).as("avg_purchases"), mean(QUANTITY).as("mean_purchases")).selectExpr("total_transactions/total_purchases", "avg_purchases", "mean_purchases").show
    /*Output of above statement
    +--------------------------------------+----------------+----------------+
    |(total_transactions / total_purchases)|   avg_purchases|  mean_purchases|
    +--------------------------------------+----------------+----------------+
    |                   0.10468738227936134|9.55224954743324|9.55224954743324|
    +--------------------------------------+----------------+----------------+
    */
  }

}
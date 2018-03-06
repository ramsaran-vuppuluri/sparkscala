import org.apache.spark.util.AccumulatorV2

class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
  private var num: BigInt = 0

  override def isZero: Boolean = this.num == 0

  override def copy(): AccumulatorV2[BigInt, BigInt] = new EvenAccumulator

  override def reset(): Unit = this.num = 0

  override def add(valueIn: BigInt): Unit = if (valueIn % 2 == 0) this.num + valueIn

  override def merge(other: AccumulatorV2[BigInt, BigInt]): Unit = this.num + other.value

  override def value: BigInt = this.num
}
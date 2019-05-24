import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}


object GroupByTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("GroupBy Test").setMaster("local[2]")
    var numMappers = 10
    var numKVPairs = 1000
    var valSize = 100
    var numReducers = 36

    val sc = new SparkContext(sparkConf)
    // 首先 init 了一个0-99 的数组： 0 until numMappers,numMappers为分区数
    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      // arr1是一个数组,每个元素都是都是一个元组.元组的格式是(Int, Array[Byte])
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      // 为第i个元素赋值
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache

    // Enforce that everything has been calculated and in cache
    pairs1.count

    println(pairs1.groupByKey(numReducers).count)

    sc.stop()
  }
}



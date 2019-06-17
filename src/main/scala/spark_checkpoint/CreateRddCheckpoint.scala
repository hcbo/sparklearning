package spark_checkpoint

import org.apache.spark.{SparkConf, SparkContext}

object CreateRddCheckpoint {
  // Unit = 表示返回值是void ,即不需要返回值
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./checkpoint")
    val rdd1 = sc.parallelize(Array((1,2),(3,4),(5,6),(7,8)),4)
    rdd1.checkpoint()
    val rdd2 = sc.textFile("/Users/hcb/Documents/s3_2.py")
    rdd2.checkpoint()

    //注意：要action才能触发checkpoint
    println("输出rdd2........."+rdd2.first())
    println("输出rdd1........."+rdd1.count())
  }

}

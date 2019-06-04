import org.apache.spark.{SparkConf, SparkContext}

object CreateRddCheckpoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./checkpoint")
    val rdd1 = sc.parallelize(Array((1,2),(3,4),(5,6),(7,8)),3)
    rdd1.checkpoint()
    //注意：要action才能触发checkpoint
    rdd1.take(1)
  }

}

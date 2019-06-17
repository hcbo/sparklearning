package spark_checkpoint

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    // 创建StreamingContext
    /* StreamingContext.getOrCreate方法会优先查看检查点路径下是否有之前保存的数据
    如果有，则根据已经保存的数据恢复失败前的计算状态；如果没有，则认为是第一次启动，调用
    functionToCreateContext方法创建一个新的StreamingContext开始任务*/
    val ssc = StreamingContext.getOrCreate("./checkpoint_streaming", functionToCreateContext _)

    // 开始
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    *
    * @param newValues 新值序列，其类型对应键值对中的值类型（这里是Int）
    * @param oldCount  之前统计的值
    * @return
    */
  def updateFunction(newValues: Seq[Int], oldCount: Option[Int]): Option[Int] = {
    val newCount = newValues.sum
    val previousCount = oldCount.getOrElse(0)
    Some(newCount + previousCount)
  }

  /**
    * StreamingContext构建函数
    *
    * @return
    */
  def functionToCreateContext(): StreamingContext = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingDemo")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // 配置检查点目录
    ssc.checkpoint("./checkpoint_streaming")

    // kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test_group",
      "auto.offset.reset" -> "latest"
    )

    // kafka主题
    val topics = Array("for_spark")

    // 从kafka创建DStream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // stream中的每一条记录都是一个ConsumerRecord，
    // public ConsumerRecord(topic: String, partition: Int, offset: Long, key: K, value: V)
    val kvs = stream.map(record => (record.value, 1))
    val count = kvs.updateStateByKey[Int](updateFunction _)

    // 打印在控制台
    count.print()
    ssc
  }
}

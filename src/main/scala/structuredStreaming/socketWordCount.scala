package structuredStreaming

import org.apache.spark.sql.SparkSession

object socketWordCount {
  def main(args: Array[String]): Unit = {

    // 创建Spark程序入口
    val sparkSession = SparkSession
      .builder()
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    // 创建监听 localhost:8000 的DataFrame流
    val lines = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "8000")
      .load()

    // 将行数据分割成单词
    /**
      * lines DataFrame代表一个包含流文本数据的无界表,这个表只有一列数据, 列名为“value”。
      * 流文本数据中的每一行都会成为表的一行。
      * 为了使用 flatMap函数，我们使用.as[String]方法将DataFrame转换为DataSet[String]
      */
    val words = lines.as[String]
      .flatMap(_.split(" "))

    // 计算 word count
    val wordCounts = words.groupBy("value").count()

    // 开始查询，把查询结果打印在控制台（完整模式）
    /**
      * 输出模式有三种，complete,append,update：
      * Complete Mode:输出所有结果
      * Append Mode: 只输出当次批次中处理的结果（未和之前处理的结果合并）
      * Update Mode: 只输出结果有变化的行
      */
    val query = wordCounts.writeStream
      .outputMode("complete")
      .option("checkpointLocation","./checkpointStructuredStreamingSocket")
      .format("console")
      .start()

    // 执行
    query.awaitTermination()

  }


}

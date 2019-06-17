package structuredStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object kafkaWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.WARN)
    val spark = SparkSession
      .builder
      .appName("StructuredStreamingWordCount")
      .master("local").getOrCreate()
    val topic = "for_spark"
    val broker = "kafka:9092"
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",broker)
      .option("subscribe",topic)
      .option("startingOffsets", "latest")
      .option("max.poll.records", 10000)
      .option("failOnDataLoss","false")
      .load()

    import spark.implicits._

    val word = df.selectExpr("CAST(key AS STRING)",
      "CAST(value AS STRING)",
      "CAST(partition as INTEGER)"
    ).as[(String,String,Integer)]
      .select("value").as[String].flatMap(_.split(" "))

    val wordcount = word.groupBy("value").count()
    val q = wordcount
      .writeStream
      .queryName("kafka_test")
      .option("checkpointLocation","./checkpoint4")
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()
    q.awaitTermination()
  }


}

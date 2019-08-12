package structuredStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object kafkaWordCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredStreamingWordCount")
      .master("local").getOrCreate()

    val topic = "sparkAlluxio"
    val broker = "kafka:9092"

    val dataStreamReader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",broker)
      .option("subscribe",topic)
      .option("startingOffsets", "latest")
      .option("max.poll.records", 10000)
      .option("failOnDataLoss","false")

    val df = dataStreamReader.load()


    import spark.implicits._

    val word = df.selectExpr("CAST(key AS STRING)",
      "CAST(value AS STRING)",
      "CAST(partition as INTEGER)"
    ).as[(String,String,Integer)]
      .select("value").as[String].flatMap(_.split(" "))

    val wordcount = word.groupBy("value").count()

    val dataStreamWriter = wordcount
      .writeStream
      .queryName("kafka_test")
      .option("checkpointLocation","alluxio://localhost:19998/neu/checkpoint_streaming1")
      .outputMode(OutputMode.Complete())
      .format("console")

    val query = dataStreamWriter.start()

    query.awaitTermination()
  }


}

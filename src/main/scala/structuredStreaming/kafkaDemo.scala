package structuredStreaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._


object kafkaDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.WARN)
    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")
      .master("local[2]")
      .getOrCreate()
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
    df.printSchema()
    val q = df.select("*")
      .writeStream
      .queryName("kafka_test")
      .outputMode(OutputMode.Append())
      .format("console")
      .start()
    q.awaitTermination()
  }


}

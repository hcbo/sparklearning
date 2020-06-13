package structuredStreaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener}

object kafkaWordCount {
  def main(args: Array[String]): Unit = {
//    System.setProperty("HADOOP_USER_NAME", "root");
    val spark = SparkSession
      .builder
      .appName("StructuredStreamingWordCount")
//      .master("local")
      .master("spark://219.216.65.161:7077")
      .getOrCreate()
    val startTime = System.currentTimeMillis()
//    println("currentTimeMillis:"+startTime)
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(startTime)))


    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        if(queryProgress.progress.batchId == 20){
          println("the total time of 20 batchs is "+(System.currentTimeMillis()-startTime)/1000 +" s")
        }
        println("Query made progress: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
      }
    })

    val topic1 = "mfsTest"
    val topic2 = "mfsTest2"
    val broker = "219.216.65.161:9092"

    val dataStreamReader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",broker)
      .option("subscribe",topic2)
//      .option("startingOffsets", "latest")
      .option("startingOffsets", "earliest")
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
      .queryName("kafka_test3")
//      .option("checkpointLocation","mfs://219.216.65.161:8888/china")
      .option("checkpointLocation","hdfs://219.216.65.161:9000/china")
//      .option("checkpointLocation","./checkpoint2020")
//      .option("checkpointLocation","alluxio://localhost:19998/neu/checkpoint_streaming2020")
      .outputMode(OutputMode.Complete())
      .format("console")

    val query = dataStreamWriter.start()

    query.awaitTermination()
  }


}

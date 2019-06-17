package spark

import org.apache.spark.sql.SparkSession

object SparkSQLDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = spark.read.json("/Users/hcb/IdeaProjects/sparkDemo/src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
  }

}

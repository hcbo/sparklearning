package spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 直接在idea中运行,不需要打包上传,方便调试
  * sbt自带scala和spark ,版本为build.sbt中指示的
  */
object WordCountLocal {
  def main(args: Array[String]): Unit = {
    val inputFile =  "/Users/hcb/Documents/testFile/wordCountInput.txt"

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")



    /**
      * SparkContext: Driver programs 通过SparkContext对象访问spark
      * SparkContext对象代表和一个集群的连接
      * 在shell中,它是自动创建好的,直接用sc就可以
      */
    val sc = new SparkContext(conf)
    /**
      * textFile就是RDD(Resilient Distributed Datasets),
      * 弹性分布式数据集, 是分布式内存的一个抽象概念
      * 虽然文件分割到不同的机器上,但是仍能用textFile这个变量代表整个文件
      */
    val textFile = sc.textFile(inputFile,4)
    /**
      *line => line.split(" ")是一个匿名函数,line相当于这个函数的参数,
      * 对每个line都进行split操作,
      * flatMap将一个元素转变成多个元素
      * 比如下边将每个line元素变成split之后的多个元素
      * 返回一个新的rdd,即lines
      */
    val lines = textFile.flatMap(line => line.split(" "))


    /**
      * word => (word, 1)
      * 把每一个word变成(word,1)这种元素
      */
    val wordCount=lines.map(word => (word, 1)).reduceByKey((a, b) => a + b)
//    val output = wordCount.saveAsTextFile("/Users/hcb/Desktop/result")
    wordCount.collect().foreach(println);

    /**
      * 上边的例子中, flatMap()和map()都是transformation操作
      * reduceByKey()和saveAsTextFile()是action操作
      */
  }
}

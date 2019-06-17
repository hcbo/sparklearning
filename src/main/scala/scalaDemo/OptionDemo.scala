package scalaDemo

object OptionDemo {
  def main(args: Array[String]): Unit = {
    val myMap: Map[String, String] = Map("key1" -> "value")
    val value1: Option[String] = myMap.get("key1")
    val value2: Option[String] = myMap.get("key2")

    println(value1) // Some("value1")
    println(value2) // None

    val a:Option[Int] = Some(5)
    val b:Option[Int] = None

// 你可以使用 getOrElse() 方法来获取元组中存在的元素或者使用其默认的值
    println("a.getOrElse(0): " + a.getOrElse(10) )
    println("b.getOrElse(10): " + b.getOrElse(10) )

    println("a.isEmpty: " + a.isEmpty )
    println("b.isEmpty: " + b.isEmpty )
  }

}

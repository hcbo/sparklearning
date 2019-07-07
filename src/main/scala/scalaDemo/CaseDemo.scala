package scalaDemo

object CaseDemo {
  def main(args: Array[String]): Unit = {
    val o: Option[Int] = Some(2)
    o match {
      case Some(x) => println(x)
      case None =>
    }
    o match {
      case x @ Some(_) => println(x)
      case None =>
    }
  }
}

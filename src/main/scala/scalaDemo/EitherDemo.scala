package scalaDemo

object EitherDemo{
  def main(args: Array[String]): Unit = {
    divideBy2(1, 3) match {
      case Left(s) => println("Answer: " + s)
      case Right(i) => println("Answer: " + i)
    }
  }


  def divideBy2(x: Int, y: Int): Either[String, Int] = {
    if(y == 0) Left("Dude, can't divide by 0")
    else Right(x / y)
  }


}


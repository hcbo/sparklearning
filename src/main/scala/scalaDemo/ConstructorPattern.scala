package scalaDemo

//构造器模式必须将类定义为case class
case class Person(name:String,age:Int)
case class Student(name:String,age:Int)
object ConstructorPattern {

  def constructorPattern(p:AnyRef)=p match {
    case Person(name,age) => "Person"
    case _ => "Other"
  }
  def main(args: Array[String]): Unit = {
    val p = new Person("NewBee",27)
    val s = new Student("hcb",18)
    println(constructorPattern(p))
    println(constructorPattern(s))
  }

}

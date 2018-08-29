/**
  * Created by sankehuang on 2018/7/9.
  */

object VariabelDemo{
  def main(args:Array[String]): Unit ={
    val i =1
    var s="hello" //val不可变，var可变
    val str:String ="itcast"
    //条件表达式
    val y=if (i>0)() else ()
    println(y)
    //块表达式
    val z={
      if (i<0) 1
      else if (i>0){
        print(111)
        1
        2
      }
    }
    println(z+"ss")
    //循环
    for (i <- 1 to 10)
      println(i)

    val array=Array("s","c","d")
    for (i<- array)
      println(i)

    //高级for循环
    //每个生成器都可以带一个条件，注意：if前面没有分号
    for (i<- 1 to 3;j<- 1 to 3 if i!=j)
      println(i,j)
    //for推导式：如果for循环的循环体以yield开始，则该循环会构建出一个集合
    //每次迭代生成集合中的一个值
    val v= for (i<-1 to 10) yield i*10
    println(v)
  }
}

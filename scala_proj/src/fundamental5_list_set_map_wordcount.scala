/**
  * Created by sankehuang on 2018/7/10.
  */
import scala.collection.mutable._
class fundamental5_list_set_map {

}
object hello {
  def main(args: Array[String]): Unit = {

    //list不可变的序列 import scala.collection.immutable._
    val list1=List(1,2,3)
    //list可变的序列 import scala.collection.mutable._
    val lst0 = ListBuffer[Int](1,2,3)
    //set scala.collection.immutable.HashSet scala.collection.mutable
    //map scala.collection.mutable



    //val lines = List("hello tom hello jerry", "hello jerry", "hello kitty").map(_.split(" ")).flatten.map((_,1)).groupBy(_._1).map(t=>(t._1,t._2.size)).toList.sortBy(_._2).reverse
    //println(lines.toBuffer)
    //println(lines)
    val lines2 = List("hello tom hello jerry", "hello jerry", "hello kitty").map(_.split(" ")).flatten.map((_,1)).groupBy(_._1)
    val lines3=lines2.mapValues(_.size)
    println(lines3)


    //map(_.split(" "))把list中的数值一一获取进行split处理，从而数值变为一个新的list
    //flatten方法，将list中的多个list值捋平变为一个大的list
    //.map((_,1)),将list中的每个值，变为（值，1）tuple
    //.groupBy(_._1)以小tuple中的第一个值进行groupby，注意，tuple是从1开始计数的，没有0
    //.map(t=>(t._1,t._2.size)) 继续对list中的所有值进行处理，处理方式为一个函数，根据size进行计数


    val lines4 = List("hello tom hello jerry", "hello jerry", "hello kitty").map(_.split(" ")).flatten.map((_,1)).groupBy(_._1).map(t=>(t._1,t._2.size))
    println(lines4)

  }
}

/**
  * Created by sankehuang on 2018/7/10.
  */
class fundamental4_map_tuple {

}
//以及各种微妙操作
object fundamental4{
  def main(args:Array[String])={
    //map 映射 创建两种方法
    val map1=Map("Tom"->24,"jerry"->32,"sda"->88)
    var map2=Map(("Tom",25),("jerry",32))
    println(map1("Tom"))
    println(map2("Tom"))
    //如果取不到给一个默认值
    println(map2.getOrElse("sf",0))
    //注意：在Scala中，有两种Map，一个是immutable包下的Map，该Map中的内容不可变；另一个是mutable包下的Map，该Map中的内容可变
    println(map1.getClass)
    map2+=("sf"->99)
    println(map2.getOrElse("sf",0))


    //tuple 元祖  就是没有value的map
    var tuple1=("hadoop",43.2,3423)
    println(tuple1)

    //var t,(a,b,c)=("hadoop",43.2,3423)
    //注意，tuple是从1开始计数的，没有0
    println(tuple1._1)



    //单个array转化为对偶的array 数组
    var arr1=Array("sf","fs")
    val arr2=Array(2,3)
    val arr3=arr1.zip(arr2)
    println(arr3.toBuffer)
    //ArrayBuffer((sf,2), (fs,3))

    //将对偶的数组转换成映射 array---->map
    var arr4=Array(("sf",3),("fs",4))
    println(arr4.toMap)



  }


}

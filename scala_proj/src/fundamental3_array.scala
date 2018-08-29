import scala.collection.mutable.ArrayBuffer

/**
  * Created by sankehuang on 2018/7/9.
  */
class fundamental3_datastructure {

}
//除了object class 这种关键字对应的变量不用等于号，其他的好像都要用
object fundamental3{
  def main(args: Array[String])={
    //初始化一个长度为8的定长数组，其所有元素均为0
    val arr=new Array[Int](8)
    //直接打印定长数组，内容为数组的hashcode值
    println(arr)
    //toBuffer会将数组转换长数组缓冲
    println(arr.toBuffer)
    println(arr.toString)

    //定长数组
    val arr2=Array("hadoop","s","sf")
    println(arr2(0))

    //变长数组（数组缓冲）,注意这里的+=，++=都是一个方法名称，只能这么写，不能写出A=A+B
    //变长数组自动import：import scala.collection.mutable.ArrayBuffer
    val ab=ArrayBuffer[Int]()
    ab+=1
    ab+=(2,3,4)
    ab++=Array(5,6,7)
    ab++=ArrayBuffer(8,9)
    //在数组某个位置插入元素用insert
    ab.insert(0, -1, 0) //第一个数字是位置，后面的全是要插入的内容
    //删除数组某个位置的元素用remove
    ab.remove(8, 2) //意思是删掉两个位置。。
    println(ab)


    //遍历数组
    //to方法：前后两个数字都包括，所以要-1
    val arr3=Array(1,2,3,4,5,6,7)
    for(i<- 0 to arr3.length-1)
      println(arr3(i))
    //until方法：前包括，后不包括
    //好用的until会生成一个Range
    //reverse是将前面生成的Range反转
    for(i<- (0 until arr3.length).reverse)
      println(arr3(i))
    //这样不好吗。。
    for (i<-arr3)
      println(i)


    //数组转化 yield和map
    val yield_test=for (i<-arr3) yield i*2
    println(yield_test.toBuffer)

    val map_test=arr3.map(_*3)
    println(map_test.toBuffer)

    //数组sum max sorted
    println(map_test.max)
    println(map_test.sum)
    println(map_test.sorted.toBuffer)
  }
}
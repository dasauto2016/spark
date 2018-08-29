/**
  * Created by sankehuang on 2018/7/9.
  */
class fundamental2 {

}
object VariabelDemo2{
  def main(args:Array[String])={
    //方法方法def。f-方法
    //:后面是返回值的类型，Unit=()
    def m1(x:Int,y:Int):Int=x*y
    println(m1(1,2))

    //函数
    val f1=(x:Int,y:Int)=>x+y
    println(f1(1,2))

    //函数：在函数式编程语言中，函数是“头等公民”，它可以像任何其他数据类型一样被传递和操作
    //函数当做参数传入的例子：怎么写这个参数：f:(Int,Int)=>Int
    def m2(f:(Int,Int)=>Int)={
      f(1,9)//如果函数作为一个参数传入后，怎么用这个参数
    }
    println(m2(f1))

    //将方法转化为头等公民函数：
    def m3(x:Int,y:Int):Int={
      x*y
    }
    val f2=m3 _ //转化方法就是空格加+_来变成函数
    println(m2(f2))
  }

}

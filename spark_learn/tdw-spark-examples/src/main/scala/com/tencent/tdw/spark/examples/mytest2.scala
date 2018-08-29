package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWFunctions._
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.sql.SparkSession

/**
  * Created by sharkdtu
  * 2015/9/1.
  */

object mytest2 {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("my_Spark-SQL-Example")
      .getOrCreate()

    import spark.implicits._

    val tdw = new TDWSQLProvider(spark,  "hlw")
    val df = tdw.table("t_dw_dc03489").cache()
    /**
      * 打印表的schema信息:
      * root
      * |-- a: byte (nullable = true)
      * |-- b: short (nullable = true)
      * |-- c: integer (nullable = true)
      * |-- d: long (nullable = true)
      * |-- e: boolean (nullable = true)
      * |-- f: float (nullable = true)
      * |-- g: double (nullable = true)
      * |-- h: string (nullable = true)
      */
    df.printSchema()
    // 打印表中的记录，默认打印前20条
    df.show()//在df中默认取样20条
//    // cache query
//    val ab = df.select("a", "b").toDF("ab_a", "ab_b").cache()
//    // 查询"ab_a"列大于1的记录
//    val filterd = ab.filter(ab("ab_a") > 1)
//    //join操作操作，默认inner join
//    val ac = df.select("a", "c").toDF("ac_a", "ac_c")
//    abc = ab.join(ac, ab("ab_a") === ac("ac_a")).select("ac_a", "ab_b", "ac_c")
//    // ab.join(ac, ab("ab_a") === ac("ac_a"), "left_outer")
//    // 分组计算，根据"a"列分组，计算"b"列最大值，"c"列平均值, "d"列的和
//    val grouped = df.groupBy("a").agg(Map(
//      "b" -> "max",
//      "c" -> "avg",
//      "d" -> "sum"
//    ))
  }
}
package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.hbase.HBaseProvider
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sharkdtu
 * 2017/5/24.
 */
object HBaseRDDTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val cmdArgs = new GeneralArgParser(args)

    val appkey = cmdArgs.getStringValue("appkey")
    val tableName = cmdArgs.getStringValue("table_name")


    val hbase = new HBaseProvider(sc)
    val rdd = sc.makeRDD(Array("1,jack,15", "2,Lily,16", "3,mike,16"))
      .map(_.split(",")).map { arr =>
      val put = new Put(Bytes.toBytes(arr(0).toInt))
      put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(2).toInt))
      (new ImmutableBytesWritable, put)
    }
    hbase.saveToTable(rdd, appkey, tableName)

    val data = hbase.table(appkey, tableName, null)
    data.collect().foreach { case (_, result) =>
      val key = Option(result.getRow).map(Bytes.toInt)
      val name = Option(result.getValue("info".getBytes, "name".getBytes)).map(Bytes.toString)
      val age = Option(result.getValue("info".getBytes, "age".getBytes)).map(Bytes.toInt)
      println("Row key: " + key + " Name: " + name + " Age: " + age)
    }
  }
}

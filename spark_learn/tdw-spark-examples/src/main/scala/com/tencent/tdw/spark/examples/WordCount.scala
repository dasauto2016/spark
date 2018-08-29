package com.tencent.tdw.spark.examples

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: WordCount <inputfile> <outputfile>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    val fs = new Path(args(0)).getFileSystem(hadoopConf)
    val fileStatus = fs.globStatus(new Path(args(0) + "*"))
    if (fileStatus != null) {
      fileStatus.foreach(file => println(file.getPath))
    } else {
      println("NULL")
    }
    val result = sc.textFile(args(0))
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    result.saveAsTextFile(args(1))
  }
}

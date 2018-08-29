package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdbank.{HippoReceiverConfig, TubeReceiverConfig, TDBankProvider}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by sharkdtu
 * 2015/9/9
 */
object TDBankDStreamTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val streamingContext = new StreamingContext(sparkConf, Seconds(30))

    val cmdArgs = new GeneralArgParser(args)

    val master = cmdArgs.getStringValue("master")
    val group = cmdArgs.getStringValue("group")
    val topic = cmdArgs.getStringValue("topic")
    val tids = cmdArgs.getCommaSplitArrayValue("tids")

    val tdbank = new TDBankProvider(streamingContext)
    // From Tube
    val receiverConf = new TubeReceiverConfig().setMaster(master)
      .setGroup(group).setTids(tids).setTopic(topic).setIncludeTid(true).setFilterOnRemote(true)
    // From hippo
    // val receiverConf = new HippoReceiverConfig().setControllerAddrs(addrs)
    //  .setGroup(group).setTopic(topic)
    val stream = tdbank.textStream(receiverConf, numReceiver = 5)

    stream.foreachRDD { rdd =>
      rdd.map(println(_)).count()
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

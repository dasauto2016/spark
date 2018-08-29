package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdbank.{TDBankProvider, TubeReceiverConfig, TubeSenderConfig}
import com.tencent.tdw.spark.toolkit.tdbank.TDBankFunctions._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by sharkdtu
 * 2017/5/24.
 */
object TDBankReceiveAndSendTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val streamingContext = new StreamingContext(sparkConf, Seconds(30))

    val cmdArgs = new GeneralArgParser(args)

    val inputMaster = cmdArgs.getStringValue("input_master")
    val inputGroup = cmdArgs.getStringValue("input_group")
    val inputTopic = cmdArgs.getStringValue("input_topic")
    val inputTids = cmdArgs.getCommaSplitArrayValue("input_tids")
    val outputBid = cmdArgs.getStringValue("output_bid")
    val outputTid = cmdArgs.getStringValue("output_tid")

    val tdbank = new TDBankProvider(streamingContext)
    // From Tube
    val receiverConf = new TubeReceiverConfig().setMaster(inputMaster)
      .setGroup(inputGroup).setTids(inputTids).setTopic(inputTopic)
    // From hippo
    // val receiverConf = new HippoReceiverConfig().setControllerAddrs(addrs)
    //  .setGroup(group).setTopic(topic)
    val stream = tdbank.bytesStream(receiverConf, numReceiver = 1)

    //    stream.foreachRDD{ rdd =>
    //      val ret = rdd.take(2)
    //      for(ba <- ret){
    //        println(new String(ba))
    //      }
    //    }

    val cnt = stream.count().map { x =>
      "count at " + System.currentTimeMillis + ": " + x
    }

    val senderConf = new TubeSenderConfig().setBid(outputBid).setTid(outputTid)
    cnt.saveToTDBank(senderConf)
    // tdbank.saveTextStreamToTDBank(cnt, senderConf)

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

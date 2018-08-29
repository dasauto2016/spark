from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pytoolkit import TDBankProvider
from pytoolkit import TubeReceiverConfig
from pytoolkit import TubeSenderConfig

if __name__ == "__main__":
    input_master = sys.argv[1].strip()
    input_group = sys.argv[2].strip()
    input_topic = sys.argv[3].strip()
    input_tids = sys.argv[4].strip().split(",")

    output_bid = sys.argv[5].strip()
    output_tid = sys.argv[6].strip()

    sc = SparkContext(appName="tdbank-dstream-test")
    ssc = StreamingContext(sc, 10)
    tdbank = TDBankProvider(ssc)
    receiverConfig = TubeReceiverConfig()\
        .setMaster(input_master)\
        .setGroup(input_group)\
        .setTopic(input_topic)\
        .setTids(input_tids)
    input_stream = tdbank.textStream(receiverConfig, 2, use_unicode=False)

    # input_stream.map(...)

    senderConfig = TubeSenderConfig().setBid(output_bid).setTid(output_tid)
    tdbank.saveTextStreamToTDBank(input_stream, senderConfig)

    ssc.start()
    ssc.awaitTermination()

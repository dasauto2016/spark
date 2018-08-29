package com.tencent.tdw.spark.benchmark

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Created by sharkdtu
 * 2017/5/2.
 *
 * Computes the PageRank of URL ids from an input file. Input file should
 * be in format of:
 * id   neighbor id
 * id   neighbor id
 * id   neighbor id
 * ...
 * where id and their neighbors are separated by space(s).
 *
 * Common spark-conf:
 *   spark.rdd.compress=true
 *   spark.scheduler.minRegisteredResourcesRatio=1
 *   spark.scheduler.maxRegisteredResourcesWaitingTime=10000000000
 */
object PageRank {

  def main(args: Array[String]) {
    if (args.length < 3) {
      throw new IllegalArgumentException(
        "Usage: PageRank <file_path> <num_iterations> <version> [<storage_level>]")
    }

    // prepare args
    val path = args(0)
    val iters = args(1).toInt
    val version = args(2)
    val storageLevel = if (args.length > 3) {
      StorageLevel.fromString(args(3))
    } else {
      StorageLevel.MEMORY_ONLY_SER
    }

    val conf = new SparkConf().setAppName("BigDataBench-PageRank")
    val sc = new SparkContext(conf)

    version match {
      case "v1" => runWithTimeTracking(runV1(sc, path, iters, storageLevel))
      case "v2" => runWithTimeTracking(runV2(sc, path, iters, storageLevel))
      case "v3" => runWithTimeTracking(runV3(sc, path, iters, storageLevel))
      case "v4" => runWithTimeTracking(runV4(sc, path, iters, storageLevel))
      case "v5" => runWithTimeTracking(runV5(sc, path, iters, storageLevel))
      case _ => throw new IllegalArgumentException(s"version error: $version")
    }
  }

  private def runWithTimeTracking[T](block: => T): T = {
    val beginTime = System.currentTimeMillis()
    val ret = block
    val endTime = System.currentTimeMillis()
    println(s"Spend time: ${endTime - beginTime}ms")
    ret
  }

  // The original version
  private def runV1(
      sc: SparkContext,
      path: String,
      iters: Int,
      storageLevel: StorageLevel): Unit = {
    val lines = sc.textFile(path)

    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey()
    links.persist(storageLevel).setName("links")
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap { case (nids, rank) =>
        val size = nids.size
        nids.map(id => (id, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    // Force action, just for trigger calculation
    ranks.foreach(_ => Unit)
  }

  // The checkpoint version
  private def runV2(
      sc: SparkContext,
      path: String,
      iters: Int,
      storageLevel: StorageLevel): Unit = {
    sc.setCheckpointDir(sc.getConf.getOption("spark.checkpoint.path")
      .getOrElse(sys.error("checkpoint dir not set"))
    )

    val lines = sc.textFile(path)
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey()
    links.persist(storageLevel).setName("links")
    var ranks = links.mapValues(v => 1.0)

    var lastCheckpointRanks: RDD[(String, Double)] = null
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap { case (nids, rank) =>
        val size = nids.size
        nids.map(id => (id, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      if (i % 5 == 0 && i != iters) {
        ranks.persist(storageLevel).setName(s"iter$i: ranks")
        ranks.checkpoint()
        // Force action, just for trigger calculation
        ranks.foreach(_ => Unit)

        if (lastCheckpointRanks != null) {
          lastCheckpointRanks.unpersist(blocking = false)
          lastCheckpointRanks.getCheckpointFile.foreach { ckp =>
            val p = new Path(ckp)
            val fs = p.getFileSystem(sc.hadoopConfiguration)
            fs.delete(p, true)
          }
        }
        lastCheckpointRanks = ranks
      }
    }
    // Final force action
    ranks.foreach(_ => Unit)
  }

  // Optimize data structure
  private def runV3(
      sc: SparkContext,
      path: String,
      iters: Int,
      storageLevel: StorageLevel): Unit = {
    sc.setCheckpointDir(sc.getConf.getOption("spark.checkpoint.path")
      .getOrElse(sys.error("checkpoint dir not set"))
    )

    val lines = sc.textFile(path)
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0).trim.toLong, parts(1).trim.toLong)
    }.distinct().groupByKey()
    links.persist(storageLevel).setName("links")

    var ranks = links.mapValues(v => 1.0)

    var lastCheckpointRanks: RDD[(Long, Double)] = null
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap { case (nids, rank) =>
        val size = nids.size
        nids.map(id => (id, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      if (i % 5 == 0 && i != iters) {
        ranks.persist(storageLevel).setName(s"iter$i: ranks")
        ranks.checkpoint()
        // Force action, just for trigger calculation
        ranks.foreach(_ => Unit)

        if (lastCheckpointRanks != null) {
          lastCheckpointRanks.unpersist(blocking = false)
          lastCheckpointRanks.getCheckpointFile.foreach { ckp =>
            val p = new Path(ckp)
            val fs = p.getFileSystem(sc.hadoopConfiguration)
            fs.delete(p, true)
          }
        }
        lastCheckpointRanks = ranks
      }
    }
    // Final force action
    ranks.foreach(_ => Unit)
  }

  // Process data skew (not recommended)
  private def runV4(
      sc: SparkContext,
      path: String,
      iters: Int,
      storageLevel: StorageLevel): Unit = {
    sc.setCheckpointDir(sc.getConf.getOption("spark.checkpoint.path")
      .getOrElse(sys.error("checkpoint dir not set"))
    )

    val lines = sc.textFile(path)
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0).trim.toLong, parts(1).trim.toLong)
    }.distinct()
    links.persist(storageLevel).setName("links")

    // Count of each id's outs
    val outCnts = links.mapValues(_ => 1).reduceByKey(_ + _)
    outCnts.persist(storageLevel).setName("out counts")

    // Init ranks
    var ranks = outCnts.mapValues(v => 1.0)

    def keyWithRandomInt[K, V](rdd: RDD[(K, V)]): RDD[((K, Int), V)] = {
      rdd.map(x => ((x._1, Random.nextInt(10)), x._2))
    }

    def expandKeyWithRandomInt[K, V](rdd: RDD[(K, V)]): RDD[((K, Int), V)] = {
      rdd.flatMap { x =>
        for (i <- 0 until 10)
          yield ((x._1, i), x._2)
      }
    }

    var lastCheckpointRanks: RDD[(Long, Double)] = null
    for (i <- 1 to iters) {
      val contribs = keyWithRandomInt(links).cogroup(
        expandKeyWithRandomInt(outCnts),
        expandKeyWithRandomInt(ranks)
      ).values.flatMap { pair =>
        for (u <- pair._1.iterator; v <- pair._2.iterator; w <- pair._3.iterator)
          yield (u, w/v)
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      if (i % 5 == 0 && i != iters) {
        ranks.persist(storageLevel).setName(s"iter$i: ranks")
        ranks.checkpoint()
        // Force action, just for trigger calculation
        ranks.foreach(_ => Unit)

        if (lastCheckpointRanks != null) {
          lastCheckpointRanks.unpersist(blocking = false)
          lastCheckpointRanks.getCheckpointFile.foreach { ckp =>
            val p = new Path(ckp)
            val fs = p.getFileSystem(sc.hadoopConfiguration)
            fs.delete(p, true)
          }
        }
        lastCheckpointRanks = ranks
      }
    }
    // Final force action
    ranks.foreach(_ => Unit)
  }

  // Process data skew (recommended)
  private def runV5(
      sc: SparkContext,
      path: String,
      iters: Int,
      storageLevel: StorageLevel): Unit = {
    sc.setCheckpointDir(sc.getConf.getOption("spark.checkpoint.path")
      .getOrElse(sys.error("checkpoint dir not set"))
    )

    val lines = sc.textFile(path)
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0).trim.toLong, parts(1).trim.toLong)
    }.distinct()
    links.persist(storageLevel).setName("links")

    // Count of each id's outs
    val outCnts = links.mapValues(_ => 1L).reduceByKey(_ + _)
      .persist(storageLevel).setName("out-counts")

    // Init ranks
    var ranks = outCnts.mapValues(_ => 1.0)
      .persist(storageLevel).setName("init-ranks")
    // Force action, just for trigger calculation
    ranks.foreach(_ => Unit)

    val skewedOutCnts = outCnts.filter(_._2 >= 1000000).collectAsMap()
    val bcSkewedOutCnts = sc.broadcast(skewedOutCnts)

    outCnts.unpersist(blocking = false)

    val skewed = links.filter { link =>
      val cnts = bcSkewedOutCnts.value
      cnts.contains(link._1)
    }.persist(storageLevel).setName("skewed-links")
    // Force action, just for trigger calculation
    skewed.foreach(_ => Unit)

    val noSkewed = links.filter { link =>
      val cnts = bcSkewedOutCnts.value
      !cnts.contains(link._1)
    }.groupByKey().persist(storageLevel).setName("no-skewed-links")
    // Force action, just for trigger calculation
    noSkewed.foreach(_ => Unit)

    links.unpersist(blocking = false)
    outCnts.unpersist(blocking = false)

    var lastCheckpointRanks: RDD[(Long, Double)] = null
    for (i <- 1 to iters) {
      val noSkewedPart = noSkewed.join(ranks).values.flatMap { case (nids, rank) =>
        val size = nids.size
        nids.map(id => (id, rank / size))
      }

      val skewedRanks = ranks.filter { rank =>
        val cnts = bcSkewedOutCnts.value
        cnts.contains(rank._1)
      }.collectAsMap()
      val bcSkewedRanks = sc.broadcast(skewedRanks)
      val skewedPart = skewed.map { link =>
        val cnts = bcSkewedOutCnts.value
        val ranks = bcSkewedRanks.value
        (link._2, ranks(link._1)/cnts(link._1))
      }

      val contribs = noSkewedPart.union(skewedPart)
      val updetedRanks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      updetedRanks.persist(storageLevel).setName(s"ranks@iter$i")
      updetedRanks.foreach(_ => Unit) // Force action

      bcSkewedRanks.destroy()
      ranks.unpersist(blocking = false)
      ranks = updetedRanks

      if (i % 5 == 0 && i != iters) {
        ranks.checkpoint()
        ranks.foreach(_ => Unit) // Force action

        if (lastCheckpointRanks != null) {
          lastCheckpointRanks.getCheckpointFile.foreach { ckp =>
            val p = new Path(ckp)
            val fs = p.getFileSystem(sc.hadoopConfiguration)
            fs.delete(p, true)
          }
        }
        lastCheckpointRanks = ranks
      }
    }
  }
}
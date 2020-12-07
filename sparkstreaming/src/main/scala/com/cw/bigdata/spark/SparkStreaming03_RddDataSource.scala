package com.cw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 创建DStream方式2：RDD队列创建DStream（了解）
  *
  * @author 陈小哥cw
  * @date 2020/8/3 13:59
  */
object SparkStreaming03_RddDataSource {
  def main(args: Array[String]): Unit = {
    // Spark的配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming03_RddDataSource")

    // 实时数据分析环境对象
    val streamingContext = new StreamingContext(sparkConf, Seconds(4))

    // 创建RDD队列
    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    // 创建QueueInputDStream
    val inputStream: InputDStream[Int] = streamingContext.queueStream(rddQueue, oneAtATime = false)

    // 处理队列中的RDD数据
    val mappedStream: DStream[(Int, Int)] = inputStream.map((_, 1))
    val reducedStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)

    reducedStream.print()

    // 启动任务
    streamingContext.start()

    // 循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5) {
      rddQueue += streamingContext.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    streamingContext.awaitTermination()

  }
}

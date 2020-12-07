package com.cw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 窗口操作:window
  *
  * @author 陈小哥cw
  * @date 2020/8/4 9:50
  */
object SparkStreaming07_Window {
  def main(args: Array[String]): Unit = {

    // Scala中的窗口
    val ints = List(1, 2, 3, 4, 5, 6)

    // 滑动窗口函数
    val intses: Iterator[List[Int]] = ints.sliding(3, 3)

    for (list <- intses) {
      println(list.mkString(","))
    }


    // Spark的配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming07_Window")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    // 以3秒为单位，将3秒内产生的数据当做一个整体得到，得到后往下执行
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 从Kafka中采集数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "node01:2181",
      "cw",
      Map("cw" -> 3)
    )

    // 窗口大小应该为采集周期的整数倍,窗口滑动的步长
    val windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(9), Seconds(3))

    // 将采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = windowDStream.flatMap(t => t._2.split(" "))

    // 将数据进行结构的转变方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 将转换结构后的数据进行聚合处理
    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    // 将结果打印出来
    wordToSumDStream.print()


    ssc.start()
    ssc.awaitTermination()

  }
}

package com.cw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 有状态的数据统计
  *
  * @author 陈小哥cw
  * @date 2020/8/3 15:48
  */
object SparkStreaming06_UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    // Spark的配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming06_UpdateStateByKey")

    // 实时数据分析环境对象
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 保存数据的状态，需要设定检查点路径
    ssc.checkpoint("checkpoint")

    // 从Kafka中采集数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "node01:2181",
      "cw",
      Map("cw" -> 3)
    )

    // 将采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = kafkaDStream.flatMap(t => t._2.split(" "))

    // 将数据进行结构的转变方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 将转换结构后的数据进行聚合处理
    //val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    // 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) => {
        val sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }

    // 将结果打印出来
    stateDStream.print()

    // 不能停止采集程序
    // streamingContext.stop()

    // 启动SparkStreamingContext
    // 1.启动采集器，开始接受数据并计算
    ssc.start()
    // 2.Driver等待采集器的执行，等待计算结束(要么手动退出,要么出现异常)才退出主程序
    ssc.awaitTermination()
  }
}

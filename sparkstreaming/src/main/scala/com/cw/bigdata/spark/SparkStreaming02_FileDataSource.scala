package com.cw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 创建DStream方式1：文件数据源创建DStream
  *
  * @author 陈小哥cw
  * @date 2020/8/3 13:43
  */
object SparkStreaming02_FileDataSource {
  def main(args: Array[String]): Unit = {
    // Spark的配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming02_FileDataSource")

    // 实时数据分析环境对象
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 监控文件夹创建DStream
    // 需要将hdfs-site.xml拷贝到resource下
    val fileDStream: DStream[String] = streamingContext.textFileStream("hdfs://nameservice1/fileStream")

    // 将采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = fileDStream.flatMap(line => line.split(" "))

    // 将数据进行结构的转变方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 将转换结构后的数据进行聚合处理
    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    // 将结果打印出来
    wordToSumDStream.print()

    // 不能停止采集程序
    // streamingContext.stop()

    // 启动SparkStreamingContext
    // 1.启动采集器，开始接受数据并计算
    streamingContext.start()
    // 2.Driver等待采集器的执行，等待计算结束(要么手动退出,要么出现异常)才退出主程序
    streamingContext.awaitTermination()


  }
}

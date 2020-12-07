package com.cw.bigdata.spark

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver

/**
  * 创建DStream方式3：自定义数据源(自定义采集器)
  *
  * @author 陈小哥cw
  * @date 2020/8/3 14:13
  */
object SparkStreaming04_MyReceiver {
  def main(args: Array[String]): Unit = {
    // Spark的配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming04_MyReceiver")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    // 以5秒为单位，将5秒内产生的数据当做一个整体得到，得到后往下执行
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 从指定的端口中采集数据
    val receiverDStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver("node01", 9999))

    // 将采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = receiverDStream.flatMap(line => line.split(" "))

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

// 声明采集器
// 1.继承Receiver
// 2.重写onStart，onStop方法
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  // 创建一个Socket
  var socket: Socket = null

  // 读数据并将数据发送给Spark
  def receive(): Unit = {
    socket = new Socket(host, port)

    // 创建一个BufferedReader用于读取端口传来的数据
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))

    // 定义一个变量，用来接收端口传过来的数据
    var line: String = null

    // 当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
    while (!isStopped() && (line = reader.readLine()) != null) {
      // 将采集的数据存储到采集器的内部进行转换
      if ("END".equals(line)) {
        return
      } else {
        this.store(line)
      }
    }
  }

  // 最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start()
  }

  override def onStop(): Unit = {
    if (socket != null) {
      socket.close()
      socket = null
    }
  }
}
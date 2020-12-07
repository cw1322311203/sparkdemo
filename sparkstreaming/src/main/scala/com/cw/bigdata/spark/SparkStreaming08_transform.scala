package com.cw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * transform操作
  *
  * @author 陈小哥cw
  * @date 2020/8/4 11:03
  */
object SparkStreaming08_transform {
  def main(args: Array[String]): Unit = {
    // Spark的配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming07_Window")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    // 以3秒为单位，将3秒内产生的数据当做一个整体得到，得到后往下执行
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 从指定的端口中采集数据
    val socketLineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    // 转换
    // TODO 代码(Driver)
    // val a=1   (只会执行一遍)
    socketLineDStream.map {
      case x => {
        // TODO 代码(Executor)
        // val a=1   (执行n遍)
        x
      }
    }

    // TODO 代码(Driver)
    // val a=1   (只会执行一遍)
    /*
    socketLineDStream.transform {
      case rdd => {
        // TODO 代码(Driver) 周期性的执行(m=采集周期)
        // val a=1   (执行一遍)
        rdd.map {
          case x => {
            // TODO 代码(Executor)
            // val a=1   (执行n遍)
            x
          }
        }
      }
    }
    */

    socketLineDStream.foreachRDD(rdd => {
      rdd.foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

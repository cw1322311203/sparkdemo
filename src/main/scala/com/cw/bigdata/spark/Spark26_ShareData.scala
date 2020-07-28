package com.cw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author 陈小哥cw
  * @date 2020/7/16 9:36
  */
object Spark26_ShareData {
  def main(args: Array[String]): Unit = {
    // 1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ShareData")

    // 2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val sum = 0
    // 使用累加器来共享变量，来累加数据

    // 创建累加器对象
    val accumulator: LongAccumulator = sc.longAccumulator
    //accumulator.add(1)//可以给累加器赋初始值

    dataRDD.foreach {
      case i => {
        // 执行累加器累加功能
        accumulator.add(i)
      }
    }

    // 获取累加器的值
    println("sum = " + accumulator.value)

    sc.stop()
  }
}

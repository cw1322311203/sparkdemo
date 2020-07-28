package com.cw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * map
  *
  * @author 陈小哥cw
  * @date 2020/7/7 16:07
  */
object Spark02_map {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    // map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    // 所有RDD算子的计算功能全部由Executor执行，如下句_ * 2是在Executor端执行的，其他代码都是在Driver端执行的
    // 外部对象，变量想传到Executor需要其能够序列化，因为外部对象和变量位于Driver端，传到Executor端涉及到网络IO传输，IO则需要其能够序列化
    val mapRDD: RDD[Int] = listRDD.map(_ * 2)

    mapRDD.collect().foreach(println)

  }

}

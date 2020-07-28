package com.cw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * mapPartitions
  *
  * @author 陈小哥cw
  * @date 2020/7/7 16:07
  */
object Spark03_mapPartitions {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapPartitions")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    // map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    // mapPartitions可以对一个RDD中所有的分区进行遍历
    // mapPartitions效率优于map算子，减少了发送到执行器执行交互次数
    // mapPartitions可能会出现内存溢出(OOM)
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(_ * 2)
    })

    mapPartitionsRDD.collect().foreach(println)

  }

}

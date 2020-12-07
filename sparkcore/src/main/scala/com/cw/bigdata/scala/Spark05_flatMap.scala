package com.cw.bigdata.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * flatMap
  *
  * @author 陈小哥cw
  * @date 2020/7/7 20:39
  */
object Spark05_flatMap {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))

    // flatMap
    // 1,2,3,4
    val flatMapRDD: RDD[Int] = listRDD.flatMap(datas=>datas)

    flatMapRDD.collect().foreach(println)

  }
}

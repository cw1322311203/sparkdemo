package com.cw.bigdata.scala.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * WordCount实现的第六种方式：combineByKey
  *
  * @author 陈小哥cw
  * @date 2020/7/9 22:55
  */
object WordCount6 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("combineByKey")

    val sc: SparkContext = new SparkContext(config)

    val lines: RDD[String] = sc.textFile("in")

    val mapRDD: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    // combineByKey实现wordcount
    mapRDD.combineByKey(
      x => x,
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x + y
    ).collect().foreach(println)

  }
}

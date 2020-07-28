package com.cw.bigdata.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * WordCount实现第一种方式:map + reduceByKey
  *
  * @author 陈小哥cw
  * @date 2020/7/9 9:59
  */
object WordCount1 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount1")

    val sc: SparkContext = new SparkContext(config)

    val lines: RDD[String] = sc.textFile("in")

    lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)


  }
}

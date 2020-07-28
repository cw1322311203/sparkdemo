package com.cw.bigdata.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * WordCount实现的第四种方式：groupByKey+map
  *
  * @author 陈小哥cw
  * @date 2020/7/9 13:32
  */
object WordCount4 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount4")

    val sc: SparkContext = new SparkContext(config)

    val lines: RDD[String] = sc.textFile("in")

    val groupByKeyRDD: RDD[(String, Iterable[Int])] = lines.flatMap(_.split(" ")).map((_, 1)).groupByKey()

    groupByKeyRDD.map(tuple => {
      (tuple._1, tuple._2.sum)
    }).collect().foreach(println)

  }
}

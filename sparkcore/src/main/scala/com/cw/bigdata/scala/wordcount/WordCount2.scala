package com.cw.bigdata.scala.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * WordCount实现第二种方式：使用countByValue代替map + reduceByKey
  *
  * 根据数据集每个元素相同的内容来计数。返回相同内容的元素对应的条数。(不必作用在kv格式上)
  * map(value => (value, null)).countByKey()
  *
  * @author 陈小哥cw
  * @date 2020/7/9 10:02
  */
object WordCount2 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount2")

    val sc: SparkContext = new SparkContext(config)

    val lines: RDD[String] = sc.textFile("in")

    lines.flatMap(_.split(" ")).countByValue().foreach(println)

  }
}

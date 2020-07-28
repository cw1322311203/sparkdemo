package com.cw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * @author 陈小哥cw
  * @date 2020/7/15 21:04
  */
object Spark21_Json {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Json")
    val sc = new SparkContext(conf)

    val json = sc.textFile("in/user.json")

    val result: RDD[Option[Any]] = json.map(JSON.parseFull)

    result.collect().foreach(println)

    sc.stop()

  }
}

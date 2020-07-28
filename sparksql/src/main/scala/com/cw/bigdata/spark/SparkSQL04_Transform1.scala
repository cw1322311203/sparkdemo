package com.cw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * RDD直接转Dataset
  *
  * @author 陈小哥cw
  * @date 2020/7/27 14:47
  */
object SparkSQL04_Transform1 {
  def main(args: Array[String]): Unit = {

    // SparkSQL

    // SparkConf
    // 创建配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL04_Transform1")

    // 创建SparkSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 涉及到RDD, DataFrame, DataSet之间的操作时，需要导入:import spark.implicits._
    // 进行转换之前，需要引入隐式转换规则
    // 这里的spark不是包名的含义,是SparkSession对象的名字
    import spark.implicits._

    // 创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40)))

    // RDD->Dataset
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = userRDD.toDS()

    val rdd1: RDD[User] = userDS.rdd

    rdd1.foreach(println)

    // 释放资源
    spark.stop()
  }
}

//case class User(id: Int, name: String, age: Int)
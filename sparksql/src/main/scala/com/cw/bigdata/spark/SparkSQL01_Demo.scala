package com.cw.bigdata.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author 陈小哥cw
  * @date 2020/7/27 14:47
  */
object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {

    // SparkSQL

    // SparkConf
    // 创建配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    // SparkContext
    val sc = new SparkContext(sparkConf)

    // SparkSession:需要导入sparksql的依赖
    // 创建SparkSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取json数据，构建DataFrame
    val df: DataFrame = spark.read.json("in/user.json")

    // 展示数据
    df.show()

    // 释放资源
    spark.stop()
  }
}

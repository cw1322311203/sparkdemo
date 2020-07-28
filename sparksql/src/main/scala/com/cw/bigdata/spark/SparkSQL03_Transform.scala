package com.cw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * RDD->DataFrame->Dataset->DataFrame->RDD 转换
  *
  * @author 陈小哥cw
  * @date 2020/7/27 14:47
  */
object SparkSQL03_Transform {
  def main(args: Array[String]): Unit = {

    // SparkSQL

    // SparkConf
    // 创建配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL03_Transform")

    // 创建SparkSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 涉及到RDD, DataFrame, DataSet之间的操作时，需要导入:import spark.implicits._
    // 进行转换之前，需要引入隐式转换规则
    // 这里的spark不是包名的含义,是SparkSession对象的名字
    import spark.implicits._

    // 创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40)))

    // RDD转换为DF
    val df: DataFrame = rdd.toDF("id", "name", "age")

    // DF转换为DS
    val ds: Dataset[User] = df.as[User]

    // Dataset转换为RDD，每个RDD的类型和Dataset
    val rdd1: RDD[User] = ds.rdd

    rdd1.foreach(user => {
      println("id=>" + user.id + ",name=>" + user.name + ",age=>" + user.age)
    })

    // DS转换为DF
    val df1: DataFrame = ds.toDF()

    // DataFrame转换为RDD,RDD类型为Row
    val rdd2: RDD[Row] = df1.rdd

    rdd2.foreach(row => {
      // 获取数据时，可以通过索引访问数据
      println("id:" + row.getInt(0) + ",name:" + row.getString(1) + ",age:" + row.getInt(2))
      // DataFrame=RDD[Row]，所以RDD[Row]和DataFrame均可通过row.getAs[类型]("字段名")来访问数据
      println("id:" + row.getAs[Int]("id") + ",name:" + row.getAs[String]("name") + ",age:" + row.getAs[Int]("age"))
    })


    // 释放资源
    spark.stop()
  }
}

case class User(id: Int, name: String, age: Int)
package com.cw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
  * 用户自定义聚合函数(强类型Dataset)
  *
  * @author 陈小哥cw
  * @date 2020/7/27 21:35
  */
object SparkSQL06_UDAF_Class {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL05_UDAF")

    // 创建SparkSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 自定义聚合函数
    // 创建聚合函数对象
    val myAgeAvg = new MyAgeAvgClassFunction

    // 将聚合函数转换为查询列
    val avgAge: TypedColumn[UserBean, Double] = myAgeAvg.toColumn.name("avgAge")

    // 读取json数据，构建DataFrame
    val df: DataFrame = spark.read.json("in/user.json")

    val userDS: Dataset[UserBean] = df.as[UserBean]

    // 应用函数
    userDS.select(avgAge).show

    spark.stop()

  }
}

case class UserBean(name: String, age: Long)

case class AvgBuffer(var sum: Long, var count: Long)

// 声明用户自定义聚合函数
// 1)继承Aggregator,设定泛型
// 2)实现方法
class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {

  // 初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  /**
    * 聚合数据:分区内的聚合操作
    *
    * @param b
    * @param a
    * @return
    */
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  // 缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  // 计算输出
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  // 自定义类型用product
  // 设定中间值类型的编码器，要转换成case类
  // Encoders.product是进行scala元组和case类转换的编码器
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  // 设定最终输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

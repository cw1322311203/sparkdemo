package com.cw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
  * 用户自定义聚合函数(弱类型DataFrame)
  *
  * @author 陈小哥cw
  * @date 2020/7/27 21:35
  */
object SparkSQL05_UDAF {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL05_UDAF")

    // 创建SparkSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 自定义聚合函数
    // 创建聚合函数对象
    val udaf = new MyAgeAvgFunction
    spark.udf.register("avgAge", udaf)

    // 读取json数据，构建DataFrame
    val df: DataFrame = spark.read.json("in/user.json")

    df.createOrReplaceTempView("user")

    spark.sql("select avgAge(age) as avgAge from user").show

    spark.stop()

  }
}

// 声明用户自定义聚合函数
// 1)继承UserDefinedAggregateFunction
// 2)实现方法
class MyAgeAvgFunction extends UserDefinedAggregateFunction {

  // 设置UDAF输入的数据类型
  override def inputSchema: StructType = {
    new StructType().add("age", LongType,nullable = true)
    // 等价于StructType(StructField("age",LongType,nullable = true)::Nil)
  }

  /**
    * 设置UDAF在聚合过程中的缓冲区保存数据的类型
    * @return
    */
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
    // 等价于StructType(StructField("sum",LongType)::StructField("count",LongType) ::Nil)
  }

  // 函数返回的数据类型
  override def dataType: DataType = DoubleType

  // 函数是否稳定:对于相同的输入是否一直返回相同的输出。
  override def deterministic: Boolean = true

  // 计算之前的缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L// 等价于buffer.update(0,0L)
    buffer(1) = 0L// 等价于buffer.update(1,0L)
  }

  // 根据查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    // count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}

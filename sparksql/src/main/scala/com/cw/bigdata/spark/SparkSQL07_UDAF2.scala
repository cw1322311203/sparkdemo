package com.cw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @author 陈小哥cw
  * @date 2020/7/28 9:28
  */
object SparkSQL07_UDAF2 {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL07_UDAF2")

    // 创建SparkSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 自定义聚合函数
    // 创建聚合函数对象
    val myAverage = new MyAgeAvg
    spark.udf.register("myAverage", myAverage)

    // 读取json数据，构建DataFrame
    val df: DataFrame = spark.read.json("in/user.json")

    df.createOrReplaceTempView("user")

    df.show()

    val result=spark.sql("select myAverage(age) as average_age from user")

    result.show()

    spark.stop()

  }
}

class MyAgeAvg extends UserDefinedAggregateFunction{

  // 聚合函数输入参数的数据类型
  override def inputSchema: StructType = {
    StructType(
      StructField("age",LongType,nullable = true)
      ::Nil
    )
    // 等价于new StructType().add("age", LongType,nullable = true)
  }

  // 聚合缓冲区中值的类型
  override def bufferSchema: StructType = {
    StructType(
      StructField("sum",LongType,nullable = true)
      ::StructField("count",LongType,nullable = true)
      ::Nil
    )
    // 等价于new StructType().add("sum", LongType).add("count", LongType)
  }

  // 最终的返回值的数据类型
  override def dataType: DataType = DoubleType

  // 确定性:对于相同的输入是否一直返回相同的输出。
  override def deterministic: Boolean = true

  // 初始化计算之前的缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 存年龄的总和
    buffer.update(0,0L) //等价于buffer(0) = 0L
    // 存年龄的个数
    buffer.update(1,0L) //等价于buffer(1) = 0L
  }

  // 分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      // 更新年龄总和
      buffer.update(0, buffer.getLong(0) + input.getLong(0)) //等价于buffer(0) = buffer.getLong(0) + input.getLong(0)
      // 更新年龄个数
      buffer.update(1, buffer.getLong(1) + 1) //等价于buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 分区间的聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer2.isNullAt(0)) {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))// 等价于buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))// 等价于buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
  }

  // 对最终聚合缓冲区中的数据进行最后一次运算
  override def evaluate(buffer: Row): Any = {
    println(buffer.getLong(0), buffer.getLong(1))
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}

package com.cw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 合并姓名
  *
  * @author 陈小哥cw
  * @date 2020/7/28 10:59
  */
object SparkSQL08_UDAF3 {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL08_UDAF3")

    // 创建SparkSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 自定义聚合函数
    // 创建聚合函数对象
    val aggregateName = new AggregateName
    spark.udf.register("aggregateName", aggregateName)

    // 读取json数据，构建DataFrame
    val df: DataFrame = spark.read.json("in/user.json")

    df.createOrReplaceTempView("user")

    df.show()

    val result=spark.sql("select aggregateName(name) as nameList from user")

    result.show()


    spark.close()

  }
}

class AggregateName extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = {
    new StructType().add("name",StringType,nullable = true)
  }

  override def bufferSchema: StructType = {
    new StructType().add("nameList",StringType,nullable = true)
    StructType(
      StructField("nameList",StringType,nullable = true)::Nil
    )
  }

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getString(0)+input.getString(0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getString(0)+buffer2.getString(0)
  }

  override def evaluate(buffer: Row): Any = {
    println(buffer.getString(0))
    buffer.getString(0)
  }
}
package com.cw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * glom
  *
  * @author 陈小哥cw
  * @date 2020/7/7 20:39
  */
object Spark08_filter {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupBy")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    // 生成数据，按照指定的规则进行过滤
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val filterRDD: RDD[Int] = listRDD.filter(x=>x%2==0)

    filterRDD.collect().foreach(println)

  }
}

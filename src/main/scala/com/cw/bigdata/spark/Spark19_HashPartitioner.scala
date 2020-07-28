package com.cw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @author 陈小哥cw
  * @date 2020/7/14 11:08
  */
object Spark19_HashPartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("HashPartitioner")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((10, "a"), (20, "b"), (30, "c"), (40, "d"), (50, "e"), (60, "f")))
    // 把分区号取出来, 检查元素的分区情况
    val rdd2: RDD[(Int, String)] = rdd1.mapPartitionsWithIndex((index, it) => it.map(x => (index, x._1 + " : " + x._2)))

    println("--------默认分区-----------")
    println(rdd2.collect.mkString(","))

    // 把 RDD1使用 HashPartitioner重新分区
    val rdd3 = rdd1.partitionBy(new HashPartitioner(5))
    // 检测RDD3的分区情况
    val rdd4: RDD[(Int, String)] = rdd3.mapPartitionsWithIndex((index, it) => it.map(x => (index, x._1 + " : " + x._2)))
    println("------使用哈希分区-----------")
    println(rdd4.collect.mkString(","))


  }
}

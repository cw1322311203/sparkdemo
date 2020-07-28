package com.cw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * checkpoint
  *
  * @author 陈小哥cw
  * @date 2020/7/14 8:58
  */
object Spark18_Checkpoint {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("checkpoint")

    val sc: SparkContext = new SparkContext(config)

    // 设定检查点的保存目录
    sc.setCheckpointDir("checkpoint")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.checkpoint()

    reduceRDD.foreach(println)
    println(reduceRDD.toDebugString)


  }
}

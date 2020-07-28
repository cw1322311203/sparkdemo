package com.cw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * distinct
  *
  * shuffle类算子
  * 将rdd中一个分区的数据打乱重组到其他不同的分区的操作，称之为shuffle操作
  *
  * @author 陈小哥cw
  * @date 2020/7/7 20:39
  */
object Spark10_distinct {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("distinct")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))

    //val distinctRDD: RDD[Int] = listRDD.distinct()// 生成12个分区文件
    // 使用distinct算子对数据去重，由于去重后会导致数据减少，所有可以改变默认分区的数量
    val distinctRDD: RDD[Int] = listRDD.distinct(2)//生成两个分区文件


    //distinctRDD.collect().foreach(println)
    distinctRDD.saveAsTextFile("output/distinct")

  }
}

package com.cw.bigdata.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * 自定义分区器
  *
  * @author 陈小哥cw
  * @date 2020/7/7 20:39
  */
object Spark14_partitionBy {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))


    println("--------HashPartitioner分区---------")
    val partRDD1: RDD[(String, Int)] = listRDD.partitionBy(new HashPartitioner(3))

    partRDD1.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    }).collect().foreach(tuple => println("分区号：" + tuple._1 + ",value：" + tuple._2))

    println("------------自定义分区--------------")
    val partRDD2: RDD[(String, Int)] = listRDD.partitionBy(new MyPartitioner(3))

    partRDD2.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    }).collect().foreach(tuple => println("分区号：" + tuple._1 + ",value：" + tuple._2))

  }
}

// 声明分区器
// 继承Partitioner类
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  // 模拟HashPartitioner
  override def getPartition(key: Any): Int = {
    key match {
      case null => 0
      case _ => {
        val rawMod = key.hashCode() % numPartitions
        rawMod + (if (rawMod < 0) numPartitions else 0)
      }
    }
  }
}
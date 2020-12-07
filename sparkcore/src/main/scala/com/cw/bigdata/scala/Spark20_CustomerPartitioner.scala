package com.cw.bigdata.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 自定义分区器
  *
  * @author 陈小哥cw
  * @date 2020/7/14 11:33
  */
object Spark20_CustomerPartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HashPartitioner")
    val sc = new SparkContext(conf)

    val arrayRDD: RDD[(Int, Int)] = sc.makeRDD(Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)))

    val parRDD: RDD[(Int, Int)] = arrayRDD.partitionBy(new CustomerPartitioner(2))

    val resultRDD: RDD[(Int, String)] = parRDD.mapPartitionsWithIndex((index, datas) => {
      datas.map(x => (index, x._1 + ":" + x._2))
    })

    println(resultRDD.collect().mkString(" "))

  }
}

class CustomerPartitioner(numParts: Int) extends Partitioner {

  //覆盖分区数
  override def numPartitions: Int = numParts

  //覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    val ckey: String = key.toString
    ckey.substring(ckey.length - 1).toInt % numParts
  }
}

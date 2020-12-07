package com.cw.bigdata.scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * sortBy
  *
  * @author 陈小哥cw
  * @date 2020/7/7 20:39
  */
object Spark13_sortBy {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sortBy")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(2,1,4,3,8,5,7,6),4)
    println("分区数："+listRDD.partitions.size)

    println("------------升序--------------")
    val sortByRDD1: RDD[Int] = listRDD.sortBy(x=>x)
    println(sortByRDD1.collect().mkString(","))

    println("------------降序--------------")
    val sortByRDD2: RDD[Int] = listRDD.sortBy(x=>x,false)
    println(sortByRDD2.collect().mkString(","))

    println("------------降序且重分区--------------")
    val sortByRDD3: RDD[Int] = listRDD.sortBy(x=>x,false,2)
    println(sortByRDD3.collect().mkString(","))
    println("分区数："+sortByRDD3.partitions.size)

  }
}

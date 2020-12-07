package com.cw.bigdata.scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * coalesce
  *
  * @author 陈小哥cw
  * @date 2020/7/7 20:39
  */
object Spark11_coalesce {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("coalesce")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    // 缩减分区数：可以简单的理解为合并分区(没有shuffle的情况下)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)

    println("缩减分区前 = "+listRDD.partitions.size)

    // 默认没有shuffle
    val coalesceRDD1: RDD[Int] = listRDD.coalesce(3)
    // 可以通过参数指定shuffle
    val coalesceRDD2: RDD[Int] = listRDD.coalesce(3,true)

    println("缩减分区后coalesceRDD1 = "+coalesceRDD1.partitions.size)
    println("缩减分区后coalesceRDD2 = "+coalesceRDD2.partitions.size)

    //coalesceRDD.saveAsTextFile("output/coalesce")

    println("***********没有shuffle打乱重组***********")
    // 没有shuffle的情况下，缩减分区相当于是在进行合并分区
    val tupleRDD1: RDD[(Int, Int)] = coalesceRDD1.mapPartitionsWithIndex((index, datas) => {
      // 等同于datas.map((index,_))
      datas.map(data => {
        (index, data)
      })
    })

    tupleRDD1.collect().foreach(tuple=>{
      println("分区号："+tuple._1+"\t分区数据："+tuple._2)
    })

    println("*************有shuffle打乱重组***********")
    // 有shuffle的情况下，缩减分区就要涉及到分区数据的打乱重组
    val tupleRDD2: RDD[(Int, Int)] = coalesceRDD2.mapPartitionsWithIndex((index, datas) => {
      // 等同于datas.map((index,_))
      datas.map(data => {
        (index, data)
      })
    })

    tupleRDD2.collect().foreach(tuple=>{
      println("分区号："+tuple._1+"\t分区数据："+tuple._2)
    })

  }
}

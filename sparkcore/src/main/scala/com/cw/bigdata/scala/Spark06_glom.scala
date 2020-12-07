package com.cw.bigdata.scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * glom
  *
  * @author 陈小哥cw
  * @date 2020/7/7 20:39
  */
object Spark06_glom {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("glom")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(3,2,4,1,5,8,7,6), 3)

    // 将一个分区的数据放到一个数组中
    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.collect().foreach(array=>{
      println("分区："+array.mkString(",")+",最大值："+array.max)
    })

  }
}

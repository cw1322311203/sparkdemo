package com.cw.bigdata.scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * repartition
  *
  * def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
  *   coalesce(numPartitions, shuffle = true)
  * }
  *
  * @author 陈小哥cw
  * @date 2020/7/7 20:39
  */
object Spark12_repartition {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("repartition")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)

    listRDD.repartition(5).glom().collect().foreach(x=>{
      println(x.mkString(","))
    })


  }
}

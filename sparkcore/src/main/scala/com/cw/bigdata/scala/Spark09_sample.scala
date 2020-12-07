package com.cw.bigdata.scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * sample
  *
  * @author 陈小哥cw
  * @date 2020/7/7 20:39
  */
object Spark09_sample {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sample")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    // 从指定的数据集合中进行抽样处理，根据不同的算法进行抽样
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    //val sampleRDD: RDD[Int] = listRDD.sample(false,0.3,1)
    val sampleRDD: RDD[Int] = listRDD.sample(true,4,1)

    sampleRDD.collect().foreach(println)

  }
}

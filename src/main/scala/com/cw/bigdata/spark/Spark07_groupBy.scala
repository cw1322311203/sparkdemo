package com.cw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * glom
  *
  * @author 陈小哥cw
  * @date 2020/7/7 20:39
  */
object Spark07_groupBy {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupBy")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    // 生成数据，按照指定的规则进行分组
    // 分组后的数据形成了对偶元祖（K-V），K表示分组的key，V表示分组的数据集合
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i=>i%2)

    groupByRDD.collect().foreach(println)

  }
}

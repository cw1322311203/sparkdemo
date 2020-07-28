package com.cw.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 需求：统计出每一个省份广告被点击次数的TOP3
  * 数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割。
  *
  * 1516609143867 6 7 64 16
  * 1516609143869 9 4 75 18
  * 1516609143869 1 7 87 12
  *
  * @author 陈小哥cw
  * @date 2020/7/9 23:38
  */
object Spark16_Practice {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")

    val sc: SparkContext = new SparkContext(config)

    // 读取数据生成RDD：时间戳，省份，城市，用户，广告
    val lines: RDD[String] = sc.textFile("in1/agent.log")

    // 按照最小粒度聚合：((省份,广告),1)
    val provinceADToOne: RDD[((String, String), Int)] = lines.map(line => {
      val fields: Array[String] = line.split(" ")
      ((fields(1), fields(4)), 1)
    })

    // 计算每个省中每个广告被点击的总数：((省份,广告),总数)
    val provinceADToSum: RDD[((String, String), Int)] = provinceADToOne.reduceByKey(_ + _)

    // 将省份作为key，广告加点击数为value：(省份,(广告,总数))
    val provinceToAdSum: RDD[(String, (String, Int))] = provinceADToSum.map(x => {
      (x._1._1, (x._1._2, x._2))
    })

    // 将同一个省份的所有广告进行聚合(省份,List((广告1,广告1总数),(广告2,广告2总数)...))
    val provinceToAdSumGroup: RDD[(String, Iterable[(String, Int)])] = provinceToAdSum.groupByKey()

    // 对同一个省份所有广告的集合进行排序并取前3条，排序规则为广告点击总数
    val provinceAdTop3: RDD[(String, List[(String, Int)])] = provinceToAdSumGroup.mapValues(x => {
      x.toList.sortWith((x, y) => {
        x._2 > y._2
      }).take(3)
      // 还可以使用这条语句取前三：x => x.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })

    // 将数据拉取到Driver端并打印
    provinceAdTop3.collect().foreach(x => {
      println("------------- 省份:" + x._1 + " --------------")
      val top3List: List[(String, Int)] = x._2
      var num = 1

      for (elem <- top3List) {
        println("排名[" + num + "]的广告编号：[" + elem._1 + "],点击数为[" + elem._2 + "]")
        num = num + 1
      }

    })

    // 关闭与spark的连接
    sc.stop()


  }
}

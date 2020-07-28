package com.cw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author 陈小哥cw
  * @date 2020/7/7 10:43
  */
object Spark01_RDD {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    // 创建RDD
    // 1)从内存中创建 makeRDD，底层实现就是parallelize
    // 默认分区数为cpu cores数和2 取大值，由于本机cpu核数为12，所有这里分区数为12
    //  override def defaultParallelism(): Int = {
    //    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
    //  }
    val listRDD1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 使用自定义分区
    val listRDD2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 2)从内存中创建parallelize
    val arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    // 3)从外部存储中创建
    // 默认情况下，可以读取项目路径，也可以读取其他路径：HDFS
    // 默认从文件中读取的数据都是字符串类型
    // 默认最小分区数为defaultParallelism和2取小值，所有这里的最小分区数为2
    // def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
    val fileRDD1: RDD[String] = sc.textFile("in")

    // 使用自定义分区
    val fileRDD2: RDD[String] = sc.textFile("in", 3)

    // 读取文件时，传递的分区参数为最小分区数，但是不一定是这个分区数，这取决于hadoop读取文件时分片规则
    // 这里文件内容为五行，5/2=2余1 ，除不尽就可能多增加一个分区
    // hadoop分区是按行读取的，分区数
    val fileRDD3: RDD[String] = sc.textFile("in", 2)



    //listRDD1.collect().foreach(println)

    // 将RDD的数据保存到文件中
    listRDD1.saveAsTextFile("output/list1") //目录中有12个分区文件
    listRDD2.saveAsTextFile("output/list2") //目录中有2个分区文件
    fileRDD1.saveAsTextFile("output/file1") //目录中有2个分区文件
    fileRDD2.saveAsTextFile("output/file2") //目录中有3个分区文件
    fileRDD2.saveAsTextFile("output/file3") //目录中有3个分区文件


  }

}

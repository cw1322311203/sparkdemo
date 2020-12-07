package com.cw.bigdata.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author 陈小哥cw
  * @date 2020/7/12 19:42
  */
object Spark17_Serializable {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val conf: SparkConf = new SparkConf().setAppName("SerDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //2.创建一个RDD
    val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello cw", "cw", "hahah"), 2)
    //3.创建一个Search对象
    val searcher = new Searcher("hello")
    //4.运用第一个过滤函数并打印结果
    val result: RDD[String] = searcher.getMatchedRDD1(rdd)
    result.collect.foreach(println)


  }
}

//需求: 在 RDD 中查找出来包含 query 子字符串的元素

// query 为需要查找的子字符串
class Searcher(val query: String) extends Serializable {
  // 判断 s 中是否包括子字符串 query
  def isMatch(s: String) = {
    s.contains(query)
  }

  // 过滤出包含 query字符串的字符串组成的新的 RDD
  def getMatchedRDD1(rdd: RDD[String]) = {
    rdd.filter(isMatch) //在executor端执行，而isMatch是Search对象中的方法，Search对象存在于Driver端，需要将Search传到executor端才可执行isMatch
    // driver向executor端传数据涉及到网络传输，网络只能传字符串，不能传对象和数字，所以需要将对象进行序列化才可进行传递
  }

  // 过滤出包含 query字符串的字符串组成的新的 RDD
  def getMatchedRDD2(rdd: RDD[String]) = {
    val query_ : String = this.query //将类变量赋值给局部变量
    rdd.filter(_.contains(query))
  }
}
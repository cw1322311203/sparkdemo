package com.cw.bigdata.scala.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * WordCount实现第三种方式：aggregateByKey或者foldByKey
  *
  * def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U): RDD[(K, U)]
  *   1.zeroValue：给每一个分区中的每一个key一个初始值；
  *   2.seqOp：函数用于在每一个分区中用初始值逐步迭代value；(分区内聚合函数)
  *   3.combOp：函数用于合并每个分区中的结果。(分区间聚合函数)
  *
  * foldByKey相当于aggregateByKey的简化操作，seqop和combop相同
  *
  * aggregateByKey
  * combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),cleanedSeqOp, combOp, partitioner)
  * foldByKey
  * combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),cleanedFunc, cleanedFunc, partitioner)
  *
  * @author 陈小哥cw
  * @date 2020/7/9 10:08
  */
object WordCount3 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount3")

    val sc: SparkContext = new SparkContext(config)

    val lines: RDD[String] = sc.textFile("in")

    lines.flatMap(_.split(" ")).map((_, 1)).aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)

    lines.flatMap(_.split(" ")).map((_, 1)).foldByKey(0)(_ + _).collect().foreach(println)


  }
}

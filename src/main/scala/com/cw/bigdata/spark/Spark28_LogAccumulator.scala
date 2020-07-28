package com.cw.bigdata.spark

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * @author 陈小哥cw
  * @date 2020/7/16 20:37
  */
// 过滤掉带字母的
object Spark28_LogAccumulator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LogAccumulator").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val accum = new LogAccumulator
    sc.register(accum, "logAccum")

    // 统计出来非纯数字, 并计算纯数字元素的和
    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if (!flag) {
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("纯数字元素之和: " + sum)//32
    for (v <- accum.value) print(v + "||")//7cd||4b||2a||
    println()
    sc.stop()
  }
}

class LogAccumulator extends AccumulatorV2[String, java.util.Set[String]] {
  private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()

  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  override def reset(): Unit = {
    _logArray.clear()
  }

  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  override def merge(other: org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]]): Unit = {
    other match {
      case o: LogAccumulator => _logArray.addAll(o.value)
    }

  }

  override def value: java.util.Set[String] = {
    java.util.Collections.unmodifiableSet(_logArray)
  }

  override def copy(): org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]] = {
    val newAcc = new LogAccumulator()
    _logArray.synchronized {
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }
}
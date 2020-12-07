package com.cw.bigdata.scala

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author 陈小哥cw
  * @date 2020/7/15 22:25
  */
object JdbcRddDemo {
  //定义一个函数，无参，返回一个jdbc的连接（用于创建JdbcRDD的第二个参数）
  val getConn: () => Connection = () => {
    DriverManager.getConnection("jdbc:mysql://cm1:3306/rdd?characterEncoding=UTF-8", "root", "123456")
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JdbcRddDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //创建RDD，这个RDD会记录以后从MySQL中读数据

    val jdbcRDD: JdbcRDD[(String, Int)] = new JdbcRDD(
      sc, //SparkContext
      getConn, //返回一个jdbc连接的函数
      "SELECT * FROM access_log WHERE id >= ? AND id < ?", //sql语句（要包含两个占位符）
      1, //第一个占位符的最小值
      5, //第二个占位符的最大值
      2, //分区数量
      rs => {
        val province = rs.getString("province")
        val count = rs.getInt("counts")
        (province, count) //将读取出来的数据保存到一个元组中
      }
    )

    //触发Action
    val result: Array[(String, Int)] = jdbcRDD.collect()
    println(result.toBuffer)

    sc.stop()

  }

}

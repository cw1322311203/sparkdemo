package com.cw.bigdata.spark

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author 陈小哥cw
  * @date 2020/7/15 21:30
  */
object Spark22_MySQL {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://cm1:3306/rdd"
    val userName = "root"
    val passWd = "123456"

    // 创建jdbcRDD访问数据库
    val sql = "select * from user where id >= ? and id <= ?"

    val jdbcRDD: JdbcRDD[Unit] = new JdbcRDD(
      sc,
      () => {
        // 获取数据库连接对象
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      sql,
      1,
      3,
      2,
      (rs) => {
        println(rs.getString("name") + "," + rs.getInt("age"))
      }
    )
    jdbcRDD.collect()

    sc.stop()
  }

}

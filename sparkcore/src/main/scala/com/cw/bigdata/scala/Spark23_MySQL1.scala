package com.cw.bigdata.scala

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author 陈小哥cw
  * @date 2020/7/15 21:30
  */
object Spark23_MySQL1 {
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

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangwu", 40)))

    dataRDD.foreachPartition(datas => {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)

      datas.foreach {
        case (username, age) => {
          val sql = "insert into user(name,age) values (?,?)"
          val statement: PreparedStatement = connection.prepareStatement(sql)

          statement.setString(1, username)
          statement.setInt(2, age)
          statement.executeUpdate()

          statement.close()
        }
      }
    })


    sc.stop()
  }

}

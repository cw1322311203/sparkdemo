package com.cw.bigdata.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @author 陈小哥cw
  * @date 2020/7/15 21:30
  */
object Spark25_HBase1 {
  def main(args: Array[String]): Unit = {
    // 1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HBase")

    // 2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    // 3.创建HBaseConf
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "cm1,cm2,cm3")
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "student")

    val dataRDD: RDD[(String, String)] = sc.makeRDD(List(("1002", "zhangsan"), ("1003", "lisi"), ("1004", "wangwu")))

    val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
      case (rowKey, name) => {
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))

        (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
      }
    }

    putRDD.saveAsHadoopDataset(jobConf)


    sc.stop()
  }

}

package com.cw.bigdata.spark

import org.apache.spark.sql._


/**
  * Dataset实现wordcount
  *
  *
  * DataSet  与RDD类似，它没有使用java 序列化或者Kryo序列化，而是使用专门的编码器序列化对象，以便对象通过网络进行处理或传输。
  * 虽然编码器和标准序列化负责将对象转换为字节，但编码器是动态生成的，并使用允许Spark执行许多操作(如过滤、排序和哈希)的格式，而无需将字节反序列化为对象。
  * 所以DataSet较RDD对比来说速度快。
  *
  * @author 陈小哥cw
  * @date 2020/7/29 8:01
  */
object SparkSQL09_DataSetWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("createDataSet").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    /**
      * 读取 集合创建DataSet
      */
    val jsonList = List[String](
      "hello spark", "hello sparksql", "hello sparkStreaming"
    )
    val linesDs: Dataset[String] = jsonList.toDS()
    val words: Dataset[String] = linesDs.flatMap(_.split(" "))

    /**
      * TODO 方法一：使用DataSet api 处理
      *
      * 这里需要导入隐式转换
      *   spark.implicits._
      * 使用agg聚合中的聚合函数，这里也要导入spark sql中的函数
      * import org.apache.spark.sql.functions._
      */

    import org.apache.spark.sql.functions._
    val groupDs: RelationalGroupedDataset = words.groupBy($"value" as "word")
    val aggDs: DataFrame = groupDs.agg(count("*") as "totalCount")
    val result: Dataset[Row] = aggDs.sort($"totalCount" desc)
    aggDs.show(100)
    result.show(100)

    /**
      * 方法二:使用sql处理
      * 这里默认 words中有个 value列，withColumnRenamed 是给列重新命名
      */

    val df: DataFrame = words.withColumnRenamed("value", "word")
    df.createOrReplaceTempView("myWords")
    spark.sql("select word,count(word) as totalCount from myWords group by word order by totalCount desc").show()


  }
}

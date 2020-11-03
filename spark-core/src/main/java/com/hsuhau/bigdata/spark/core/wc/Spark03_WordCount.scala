package com.hsuhau.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    //    Application
    //    Spark框架

    // TODO 建立和Spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf);

    // TODO 执行业务操作

    val lines: RDD[String] = sparkContext.textFile(path = "datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )

    // Spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现
    // reduceBy：相同的key的数据，可以对valuejinxing reduce juhe

    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(
      _ + _
    )

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println);

    // TODO 关闭连接
    sparkContext.stop();
  }
}

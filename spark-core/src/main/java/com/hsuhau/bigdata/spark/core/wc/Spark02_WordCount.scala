package com.hsuhau.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
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

    val worldGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      t => t._1
    )

    val wordToCount: RDD[(String, Int)] = worldGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
        (word, list.size)
      }
    }

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println);

    // TODO 关闭连接
    sparkContext.stop();
  }
}

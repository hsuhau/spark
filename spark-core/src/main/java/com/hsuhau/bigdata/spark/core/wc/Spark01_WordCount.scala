package com.hsuhau.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //    Application
    //    Spark框架

    // TODO 建立和Spark框架的连接
//    val sparkConf = new SparkConf().setMaster("spark://fa958038cc24:7077").setAppName("WordCount").setIfMissing("spark.driver.host", "10.181.12.235")
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf);

    // TODO 执行业务操作

    //    1.读取文件，获取一行一行的数据
    //    hello world
    val lines: RDD[String] = sparkContext.textFile(path = "datas")

    //   2.将一行数据进行拆分，形成一个一个的单词（分词）
    //    hello world => (hello, world, hello, world)
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //    3.将数据根据单词进行分组，便于统计
    //    (hello, hello, hello),(word, word)
    val worldGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    //    4.对分组后的数据进行转换
    //    (hello, hello, hello),(world, world)
    //    (hello, 3),(world, 2)
    val wordToCount: RDD[(String, Int)] = worldGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //    5.将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println);

    // TODO 关闭连接
    sparkContext.stop();
  }
}

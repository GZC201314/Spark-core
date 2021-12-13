package org.gzc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    /* 1.建立和Sapark框架的连接*/

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
    val context = new SparkContext(sparkConf);

    /* 2.执行业务操作*/

    //读取文件
    val lines:RDD[String] = context.textFile("src/main/resources/datas")
    //split 行数据

    val words:RDD[String] = lines.flatMap(_.split(" "))

    //将数据根据单词进行分组

    val wordToOne = words.map(
      word => (word, 1)
    )

    //聚合
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey((x, y) => {
      x + y
    })
    //输出结果
    val tuples = wordToCount.collect()
    tuples.foreach(println)
    /* 3.关闭连接*/
    context.stop();
  }
}

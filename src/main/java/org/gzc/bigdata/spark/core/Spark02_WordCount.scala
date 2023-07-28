package org.gzc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    /* 1.建立和Sapark框架的连接*/

    val sparkConf = new SparkConf().setMaster("local[0]").setAppName("WordCount");
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

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)

    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (str, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }
    //输出结果
    val tuples = wordToCount.collect()
//    tuples.foreach(println)
    /* 3.关闭连接*/
    context.stop();
  }
}

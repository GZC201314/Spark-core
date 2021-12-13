package org.gzc.bigdata.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Persist")
    val context = new SparkContext(sparkConf)

    val rdd: RDD[String] = context.makeRDD(List("Hello Spark", "Hello Scala"))
    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    val resultRDD = wordToOneRDD.reduceByKey(_ + _)
    resultRDD.collect().foreach(println)
    /**虽然复用了之前的RDD但是，底层还是执行了之前的RDD的生成操作，本质上还是没有复用，
    因为在RDD传给reducebykey之后当前的RDD已经从内存中消失了，如果想要复用RDD可以使用持久化的操作，持久化的操作包括两种
     1. 放到缓存中
     2. 放到文件中
     */
    val groupRDD: RDD[(String, Iterable[Int])] = wordToOneRDD.groupByKey()
    groupRDD.collect().foreach(println)
  }
}

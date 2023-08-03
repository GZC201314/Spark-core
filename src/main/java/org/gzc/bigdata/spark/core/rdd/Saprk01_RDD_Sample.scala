package org.gzc.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Saprk01_RDD_Sample {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Group")
    val context = new SparkContext(sparkConf)
    val rdd = context.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    //第一个参数 是否有放回的抽取
    //第二个参数 每个数据被抽到的概率
    //第三个参数 种子 如何种子固定，抽取的样本一样
    println(rdd.sample(withReplacement = true, 2).collect().mkString(","))
    context.stop()
  }

}

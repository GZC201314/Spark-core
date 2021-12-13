package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_transform_test {



  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)


    val rdd: RDD[Int] = context.makeRDD(List(1, 2, 3, 4),2)

    /*在分区的数据加载完成后才开始执行*/
    /*需要返回一个迭代器对象*/
    val mpRDD: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })
    mpRDD.collect().foreach(println)

    context.stop()

  }
}

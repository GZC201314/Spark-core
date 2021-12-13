package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_transform_Repartition {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)


    val rdd = context.makeRDD(List(1, 2, 3, 4, 5, 6), 3);
    // 扩大分区，底层实现还是Coalesce，主要适用于区分，便于理解，语法糖
    val value: RDD[Int] = rdd.repartition(2)
    value.saveAsTextFile("output")
    context.stop()

  }
}

package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_transform_DoubleRDD {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = context.makeRDD(List(3, 4, 5, 6))

    //交集

    val intersectionRDD: RDD[Int] = rdd1.intersection(rdd2)
    println(intersectionRDD.collect().mkString(","))

    //并集,重复的数据不会合并

    val unionRDD: RDD[Int] = rdd1.union(rdd2)
    println(unionRDD.collect().mkString(","))

    //差集

    val subtractRDD: RDD[Int] = rdd1.subtract(rdd2)
    println(subtractRDD.collect().mkString(","))

    //拉链 将对应位置相同的数据放到一起

    val zipRDD: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(zipRDD.collect().mkString(","))

    context.stop()

  }
}

package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_transform_DoubleRDD_Zip {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = context.makeRDD(List(1, 2, 3, 4),2)
    val rdd2: RDD[Int] = context.makeRDD(List(3, 4, 5, 6),2)

    //拉链 将对应位置相同的数据放到一起,要求分区数量和分区内数据个数 保持一致

    val zipRDD: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(zipRDD.collect().mkString(","))

    context.stop()

  }
}

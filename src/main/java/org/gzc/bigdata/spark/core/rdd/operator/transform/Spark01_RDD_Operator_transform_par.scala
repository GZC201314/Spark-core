package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_transform_par {



  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)


    val rdd: RDD[Int] = context.makeRDD(List(1, 2, 3, 4),1)

    val rdd1: RDD[Int] = rdd.map((num) => {
      println("************" + num)
      num
    })
    val rdd3: RDD[Int] = rdd1.map((num) => {
      println("===============" + num)
      num
    })
    rdd3.collect()
    context.stop()

  }
}

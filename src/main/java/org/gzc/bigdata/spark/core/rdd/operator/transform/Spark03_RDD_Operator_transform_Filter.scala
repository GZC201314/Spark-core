package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_transform_Filter {



  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)


//    val rdd = context.makeRDD(List(1,2,3,4,5,6,7,8),2)
//
//
//    val filterRDD: RDD[Int] = rdd.filter((num) => {
//      num % 2 == 1
//    })
//    filterRDD.collect().foreach(println)

    val rdd: RDD[String] = context.textFile("src/main/resources/datas/apache.log")

    val date: RDD[String] = rdd.filter(
      line => {
        val dates: Array[String] = line.split(" ")
        dates(3).startsWith("17/05/2015")
      }
    )

    date.collect().foreach(println)


    context.stop()

  }
}

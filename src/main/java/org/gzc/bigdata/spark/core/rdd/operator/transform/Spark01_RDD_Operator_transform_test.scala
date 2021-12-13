package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_transform_test {



  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)

    val logs: RDD[String] = context.textFile("src/main/resources/datas/apache.log")

    val urlRDD: RDD[String] = logs.map(
      line => {
        val strings: Array[String] = line.split(" ")
        strings(6)
      }
    )
    urlRDD.collect().foreach(println)

    context.stop()

  }
}

package org.gzc.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Save")
    val context = new SparkContext(sparkConf)
    val rdd: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))
    val value = rdd.mapPartitionsWithIndex((index, iter) => {
      iter.map(f = num => {
        List(index,num)
      })
    })

    value.collect().foreach(println)
//
//    var sum: Int = 0;
//    rdd.foreach(
//      num => {
//        sum += num
//      }
//    )
//    println("sum = " + sum)

    context.stop()
  }
}

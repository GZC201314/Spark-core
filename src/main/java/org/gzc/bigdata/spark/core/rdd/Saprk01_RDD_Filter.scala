package org.gzc.bigdata.spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Saprk01_RDD_Filter {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Group")
    val context = new SparkContext(sparkConf)
    val rdd = context.makeRDD(List(1, 2, 3, 4));

    val tuples = rdd.filter((num: Int) => (num % 2 == 0)).collect()
    tuples.foreach(println)
    context.stop()
  }

}

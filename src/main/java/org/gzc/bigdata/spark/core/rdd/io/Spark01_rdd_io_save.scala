package org.gzc.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_rdd_io_save {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Save")
    val context = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = context.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))

    rdd.saveAsTextFile("output/output1")
    rdd.saveAsObjectFile("output/output2")
    rdd.saveAsSequenceFile("output/output3")

    context.stop()
  }
}

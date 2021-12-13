package org.gzc.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_rdd_io_load {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Save")
    val context = new SparkContext(sparkConf)
    val rdd1 = context.textFile("output/output1")
    rdd1.collect().foreach(println)

    val rdd2: RDD[(String,Int)] = context.objectFile[(String,Int)]("output/output2")
    rdd2.collect().foreach(println)

    val rdd3: RDD[(String, Int)] = context.sequenceFile[String, Int]("output/output3")
    rdd3.collect().foreach(println)
    context.stop()
  }
}

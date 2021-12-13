package org.gzc.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_rdd {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism","3")
    val context = new SparkContext(sparkConf)
    /*创建RDD 有两种方法*/
    //    val rdd1: RDD[Int] = context.parallelize(List(1, 2, 3, 4))
    /*第二个参数是数据分的份数 ，需要在SparkConf中配置，如果没有配置*/
    val rdd2: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))
    rdd2.saveAsTextFile("src/main/resources/output")
  }
}

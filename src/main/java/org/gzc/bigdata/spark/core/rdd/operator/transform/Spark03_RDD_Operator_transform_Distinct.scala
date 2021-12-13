package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_transform_Distinct {



  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)


    val rdd = context.makeRDD(List(1,2,3,4,1,2,3,4));

    val rddDistinct: RDD[Int] = rdd.distinct()
    rddDistinct.collect().foreach(println)
    context.stop()

  }
}

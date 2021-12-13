package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Operator_transform {



  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)
    /*创建RDD 有两种方法*/
//        val rdd1: RDD[Int] = context.parallelize(List(1, 2, 3, 4))
    /*第二个参数是数据分的份数 ，需要在SparkConf中配置，如果没有配置*/
    val rdd2: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))

    rdd2.map((num:Int)=>{num*2}).collect().foreach(println)

    context.stop()

  }
}

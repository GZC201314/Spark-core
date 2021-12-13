package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_transform_test2 {



  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)


    val rdd = context.makeRDD(List(List(1,2,3), 4, List(5,6,7)))

    val dataList = rdd.flatMap(
      data =>{
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    )

    dataList.collect().foreach(println)


    context.stop()

  }
}

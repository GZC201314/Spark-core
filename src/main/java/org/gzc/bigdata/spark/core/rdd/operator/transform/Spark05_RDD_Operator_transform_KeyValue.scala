package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark05_RDD_Operator_transform_KeyValue {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)
    val rdd: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))

    val keyValueRDD: RDD[(Int, Int)] = rdd.map((num) => {
      (num, 1)
    })
    /*partitionBy 根据指定的分区规则对数据进行重分区*/
    keyValueRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")
    context.stop()

  }
}

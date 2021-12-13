package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_transform_KeyValue_groupBykey {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)
    val rdd = context.makeRDD(List(
      ("a",1),
      ("a",2),
      ("a",3),
      ("b",4),
    ))
    /*groupByKey: 将数据源中的数据，相同key的数据分到一个组中，形成一个对偶元组
    *             元组中的第一个元素就是key
    *             元组中的第二个元素就是相同key对应的value集合
    * */
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupByKeyRDD.collect().foreach(println)

    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    groupByRDD.collect().foreach(println)
    context.stop()

  }
}

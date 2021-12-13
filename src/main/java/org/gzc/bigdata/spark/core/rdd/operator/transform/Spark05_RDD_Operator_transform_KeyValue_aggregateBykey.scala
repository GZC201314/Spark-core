package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_transform_KeyValue_aggregateBykey {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)
    val rdd = context.makeRDD(List(
      ("a",1),
      ("a",2),
      ("b",4),
      ("b",4),
      ("a",6)
    ),2)

    // 在计算机科学中，柯里化（Currying）是把接受多个参数的函数变换成接受一个单一参数(最初函数的第一个参数)的函数，
    // 并且返回接受余下的参数且返回结果的新函数的技术。
    // aggregateByKey 存在函数的柯里化
    // 第一个参数列表 需要传递一个参数，表示为初始值
    //                主要用于当遇到第一个Key的时候，和value进行分区内计算
    // 第二个参数列表
    //      第一个参数表示分区内计算规则
    //      第二个参数表示分区间计算规则
//    val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(Int.MinValue)((x, y) => {
//      math.max(x, y)
//    },
//      (x, y) => x + y
//    )
//    aggregateByKeyRDD.collect().foreach(println)
    val aggregateByKeyRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    val value: RDD[(String, Int)] = aggregateByKeyRDD.map(
      (item) => {
        (item._1, item._2._1 / item._2._2)
      }
    )

    value.collect().foreach(println)

    context.stop()
  }
}

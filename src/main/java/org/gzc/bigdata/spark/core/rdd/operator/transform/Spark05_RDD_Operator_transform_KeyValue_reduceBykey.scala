package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark05_RDD_Operator_transform_KeyValue_reduceBykey {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)
    val rdd = context.makeRDD(List(
      ("a",1),
      ("a",2),
      ("a",3),
      ("b",4),
    ))
    /*reduceByKey 中如果key的数据只有一个，是不会进行聚合运算的
    *
    * shuffle 的时候Spark都会执行落盘操作，减少内存的使用量
    *
    * reduceByKey 支持分区内预聚合功能（combine），可以有效减少shuffle时落盘的数据量，提升shuffle的性能
    *
    * 所以reduceBykey 要比groupBykey的性能要好
    * */
    val reduceByKeyRDD: RDD[(String, Int)] = rdd.reduceByKey((x, y) => {
      x + y
    })
    reduceByKeyRDD.collect().foreach(println)
    context.stop()

  }
}

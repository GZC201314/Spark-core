package org.gzc.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("广播变量")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3),
      ("d", 4),
    ))
    val rdd2 = sc.makeRDD(List(
      ("a", 5),
      ("b", 6),
      ("c", 7),
      ("d", 8),
    ))

    val map = mutable.Map(
      ("a", 5),
      ("b", 6),
      ("c", 7),
      ("d", 8),
    )

    val bcMap: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)


    val bcRDD = rdd1.map {
      case (word, count) => {
        (word, (bcMap.value.getOrElse(word, 0), count))
      }
    }
    bcRDD.collect().foreach(println)

    sc.stop()
  }
}

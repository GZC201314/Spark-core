package org.gzc.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Save")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 获取系统累加器，Spark默认就提供了简单数据聚合的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("Sum")

    //    sc.doubleAccumulator
    //sc.collectionAccumulator()
    // 少加问题： 转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    val mapRDD: RDD[Unit] = rdd.map(
      num => {
        sumAcc.add(num)
      }
    )
    println("sum = " + sumAcc.value)

    // sum = 0
    // 多加问题： 转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    mapRDD.collect()
    mapRDD.collect()
    println("sum = " + sumAcc.value)
    sc.stop()
  }
}

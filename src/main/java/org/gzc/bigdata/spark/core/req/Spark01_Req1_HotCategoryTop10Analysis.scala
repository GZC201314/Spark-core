package org.gzc.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {
    // TODO Top 10 热门品种
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Top10")
    val sc = new SparkContext(sparkConf)
    //1. 读取原始数据
    val actionRDD: RDD[String] = sc.textFile("src/main/resources/datas/user_visit_action.txt")
    actionRDD.cache()
    //2. 统计品种的点击数量

    val clickActionRDD: RDD[String] = actionRDD.filter((action) => {
      val strings: Array[String] = action.split("_")
      strings(6) != "-1"
    })

    val clickActionResultRDD: RDD[(String, Int)] = clickActionRDD.map((action) => {
      val datas: Array[String] = action.split("_")
      (datas(6), 1)
    }).reduceByKey(_ + _)


    //3. 统计品种的下单数量
    val orderActionRDD: RDD[String] = actionRDD.filter((action) => {
      val strings: Array[String] = action.split("_")
      strings(8) != "null"
    })
    val orderActionResultRDD: RDD[(String, Int)] = orderActionRDD.flatMap((action) => {
      val strings: Array[String] = action.split("_")
      val cids: Array[String] = strings(8).split("-")
      cids.map(id => (id, 1))
    }).reduceByKey(_ + _)

    //4. 统计品种的支付数量

    val payActionRDD: RDD[String] = actionRDD.filter((action) => {
      val strings: Array[String] = action.split("_")
      strings(10) != "null"
    })
    val payActionResultRDD: RDD[(String, Int)] = payActionRDD.flatMap((action) => {
      val strings: Array[String] = action.split("_")
      val cids: Array[String] = strings(10).split("-")
      cids.map(id => (id, 1))
    }).reduceByKey(_ + _)

    //5. 将品种进行排序，并且取前10名
    // 点击数量排序，下单数量排序，支付数量排序

    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickActionResultRDD.cogroup(orderActionResultRDD, payActionResultRDD)

    val resultRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        val clickIterator: Iterator[Int] = clickIter.iterator
        if (clickIterator.hasNext) {
          clickCnt = clickIterator.next()
        }
        var orderCnt = 0
        val orderIterator: Iterator[Int] = orderIter.iterator
        if (orderIterator.hasNext) {
          orderCnt = orderIterator.next()
        }
        var payCnt = 0
        val payIterator: Iterator[Int] = payIter.iterator
        if (payIterator.hasNext) {
          payCnt = payIterator.next()
        }
        (clickCnt, orderCnt, payCnt)
      }
    }
    val top10ResultRDD: Array[(String, (Int, Int, Int))] = resultRDD.sortBy(_._2, false).take(10)

    //6. 输出结果
    top10ResultRDD.foreach(println)

    sc.textFile("src/main/resources/datas")

    sc.stop()
  }
}

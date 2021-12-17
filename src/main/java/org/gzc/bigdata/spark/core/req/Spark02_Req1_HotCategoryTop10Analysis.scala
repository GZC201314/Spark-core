package org.gzc.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Req1_HotCategoryTop10Analysis {

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

    val rdd1: RDD[(String, (Int, Int, Int))] = clickActionResultRDD.map {
      case (str, i) => {
        (str, (i, 0, 0))
      }
    }
    val rdd2: RDD[(String, (Int, Int, Int))] = orderActionResultRDD.map {
      case (str, i) => {
        (str, (0, i, 0))
      }
    }
    val rdd3: RDD[(String, (Int, Int, Int))] = payActionResultRDD.map {
      case (str, i) => {
        (str, (0, 0, i))
      }
    }

    val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)

    val resultRDD: RDD[(String, (Int, Int, Int))] = sourceRDD.reduceByKey((item1, item2) => {
      (item1._1 + item2._1, item1._2 + item2._2, item1._3 + item2._3)
    })

    val top10ResultRDD: Array[(String, (Int, Int, Int))] = resultRDD.sortBy(_._2, false).take(10)

    //6. 输出结果
    top10ResultRDD.foreach(println)


    sc.stop()
  }
}

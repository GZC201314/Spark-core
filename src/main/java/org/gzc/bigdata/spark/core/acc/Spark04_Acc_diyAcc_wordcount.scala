package org.gzc.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_diyAcc_wordcount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCountAcc")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Scala", "Hello", "Spark"))
    //创建累加器对象
    val accumulator = new MyAccumulator
    //注册累加器对象
    sc.register(accumulator, "wordCountAcc")
    rdd.foreach(
      word => {
        accumulator.add(word)
      }
    )
    println(accumulator.value)
    sc.stop()
  }

  /**
   * 自定义数据累加器：WordCount
   * IN ：累加器输入的内容
   * OUT ：累加器返回的数据内容
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap = mutable.Map[String, Long]()

    // 判断是否是初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    // 获取累加器需要计算的数据
    override def add(word: String): Unit = {
      val count: Long = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, count)
    }

    /**
     * Driver 合并多个累加器
     *
     * @param other
     */
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map: mutable.Map[String, Long] = this.wcMap
      val map2: mutable.Map[String, Long] = other.value
      map2.foreach {
        case (word, count) => {
          val newCount: Long = map.getOrElse(word, 0L) + count
          map.update(word, newCount)
        }
      }
    }

    /**
     * 获取累加器的结果
     *
     * @return
     */
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}

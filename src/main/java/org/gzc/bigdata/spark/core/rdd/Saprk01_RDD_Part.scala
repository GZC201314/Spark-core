package org.gzc.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Saprk01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Persist")
    val context = new SparkContext(sparkConf)
    val rdd: RDD[(String, String)] = context.makeRDD(List(
      ("nba", "1"),
      ("cba", "1"),
      ("wnba", "1"),
      ("wcba", "1")
    ));
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new Mypartitioner)

    partRDD.saveAsTextFile("output")

    context.stop()
  }

  /**
   * 自定义分区器
   */
  class Mypartitioner extends Partitioner {
    /**
     * 分区的数量
     *
     * @return
     */
    override def numPartitions: Int = 3

    //返回数据的分区索引
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2

      }
    }
  }

}

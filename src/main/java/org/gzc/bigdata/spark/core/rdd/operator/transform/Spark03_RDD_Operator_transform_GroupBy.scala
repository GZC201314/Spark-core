package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_transform_GroupBy {



  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)


    val rdd = context.makeRDD(List("Hadoop","Spark","Scala","Hive"),2)

    /*List => Int*/
    /*Int => List*/

    val headRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    headRDD.saveAsTextFile("output")

    context.stop()

  }
}

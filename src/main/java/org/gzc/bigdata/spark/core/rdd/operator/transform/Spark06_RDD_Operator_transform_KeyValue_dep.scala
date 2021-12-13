package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_transform_KeyValue_dep {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("血缘关系")
    val context = new SparkContext(sparkConf)

    val line: RDD[String] = context.textFile("src/main/resources/datas/1.txt")
    println(line.toDebugString)
    println("=================================================")
    val wordRDD: RDD[String] = line.flatMap(_.split(" "))
    println(wordRDD.toDebugString)
    println("=================================================")

    val wordToOne: RDD[(String, Int)] = wordRDD.map((_, 1))
    println(wordToOne.toDebugString)
    println("=================================================")

    val resultRDD: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(resultRDD.toDebugString)
    println("=================================================")

    resultRDD.collect().foreach(println)
    context.stop()

  }
}

package org.gzc.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Persist")
    val context = new SparkContext(sparkConf)
    context.setCheckpointDir("backup")
    val rdd: RDD[String] = context.makeRDD(List("Hello Spark", "Hello Scala"))
    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map(item =>{
      println("**************************")
      (item,1)
    })
    /**虽然复用了之前的RDD但是，底层还是执行了之前的RDD的生成操作，
     * 本质上还是没有复用，因为在RDD传给reducebykey之后当前的RDD已经从内存中消失了，
     * 如果想要复用RDD可以使用持久化的操作，持久化的操作包括两种
     1. 放到缓存中 不安全，可能导致内存溢出
     2. 放到文件中 效率低，因为有磁盘的IO操作
     */
    //1. 放到内存中方法
    // wordToOneRDD.cache()
    //2. 放到磁盘中
    //持久化操作只会在行动算子执行时操作
    // 如果指定检查点的保存路径
    // 作业执行完之后，文件也不会被删除，一般保存在分布式存储系统中
    wordToOneRDD.checkpoint()
    val resultRDD = wordToOneRDD.reduceByKey(_ + _)
    resultRDD.collect().foreach(println)
    val groupRDD: RDD[(String, Iterable[Int])] = wordToOneRDD.groupByKey()
    groupRDD.collect().foreach(println)
  }
}

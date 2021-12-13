package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_transform_Coalesce {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)

    /*
    根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
    当Spark程序中，存在过多的小任务的时候，可以通过coalesce方法，收缩合并分区，减少分区的个数，减少任务的调度成本。
    * */
    val rdd = context.makeRDD(List(1, 2, 3, 4, 5, 6), 3);
    // coalesce 算子默认 不会打乱分区重新组合
    // 这种情况下的缩减分区可能导致数据不均衡，出现数据倾斜，如果想要让数据均衡，可以进行shuffle处理
    val rdd2: RDD[Int] = rdd.coalesce(2, shuffle = true)
    rdd2.saveAsTextFile("output")
    context.stop()

  }
}

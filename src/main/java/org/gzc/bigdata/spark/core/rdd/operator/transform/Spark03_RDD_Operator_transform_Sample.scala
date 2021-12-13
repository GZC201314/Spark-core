package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_transform_Sample {



  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)


    val rdd = context.makeRDD(List(1,2,3,4,5,6,7,8,9,10));

    //sample 算子需要爱传递三个参数
    /*
    * 1. 第一个参数表示，抽取数据后是否将数据放回 true 返回 false 不放回
    *
    * 2. 第二个参数表示，数据源中每条数据被抽取的概率
    *
    * 3. 第三个参数表示，抽取数据时随机算法的种子,种子确定了随机值
    *                    如果不传递第三个参数，那么使用的当前的系统时间
    * */
    println(    rdd.sample(
      false,
      fraction = 0.4
    ).collect().mkString(","))

    context.stop()

  }
}

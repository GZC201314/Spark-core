package org.gzc.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_transform_SortBy {



  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)


    val rdd = context.makeRDD(List(("2",1),("11",2),("tets",5),("gzc",3)),2);

    /*sortBy 方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以设置为降序排列
    *
    * sortBy 默认情况下不会改变分区，但是中间存在shuffle操作
    * */
    val sortByRDD= rdd.sortBy(num => num._1,false)
    sortByRDD.collect().foreach(println)
    context.stop()

  }
}

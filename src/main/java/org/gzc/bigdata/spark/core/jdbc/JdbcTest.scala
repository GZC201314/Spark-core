package org.gzc.bigdata.spark.core.jdbc

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties
class JdbcTest {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[0]").setAppName("WordCount");
    val context = new SparkContext(sparkConf);

    val url = "jdbc:mysql://localhost:12306"
    val table = "bsm"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "GZCabc123")
  }


}

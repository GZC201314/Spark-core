package org.gzc.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_Req1_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {
    // TODO Top 10 热门品种
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Top10")
    val sc = new SparkContext(sparkConf)
    //1. 读取原始数据
    val actionRDD: RDD[String] = sc.textFile("src/main/resources/datas/user_visit_action.txt")
    //2. 统计品种的点击数量
    val hotCategoryAccumulator = new HotCategoryAccumulator
    sc.register(hotCategoryAccumulator,"hotCategoryAccumulator")

    actionRDD.foreach(
      action =>{
        val datas: Array[String] = action.split("_")
        if(datas(6) != "-1"){
          //点击情况
          hotCategoryAccumulator.add(datas(6),"click")
        }else if(datas(8) != "null"){
          // 下单情况
          val ids: Array[String] = datas(8).split(",")
          ids.foreach((id) =>{
            hotCategoryAccumulator.add(id,"order")
          })
        }else if(datas(10) != "null"){
          // 支付情况
          val ids: Array[String] = datas(10).split(",")
          ids.foreach((id) =>{
            hotCategoryAccumulator.add(id,"pay")
          })
        }
      }
    )

    val value: mutable.Map[String, HotCategory] = hotCategoryAccumulator.value
    val categories: mutable.Iterable[HotCategory] = value.map(_._2)
    val source: List[HotCategory] = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true;
          } else if (left.orderCnt == right.orderCnt) {
            if (left.payCnt > right.payCnt) {
              true
            } else {
              false
            }
          } else {
            false;
          }
        } else {
          false
        }
      }
    )

    source.take(10).foreach(println)
    sc.stop()
  }

  /*var 表示当前的变量可以被修改*/
  case class HotCategory(cid:String,var clickCnt:Int,var orderCnt:Int,var payCnt:Int)
  /**
   * 自定义累加器
   * 1 继承 AccumulatorV2，定义泛型
   *
   * IN：（品类ID，行为类型）
   * OUT：mutable.map[String,HotCategory]
   *
   */
  class HotCategoryAccumulator  extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{

    private val hotCategory = mutable.Map[String,HotCategory]()

    override def isZero: Boolean = {
      hotCategory.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      hotCategory.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category: HotCategory = hotCategory.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if(actionType == "click"){
        category.clickCnt+=1;
      }else if(actionType == "order"){
        category.orderCnt+=1;
      }else if(actionType == "pay"){
        category.payCnt +=1;
      }
      hotCategory.update(cid,category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hotCategory
      var map2 = other.value
      map2.foreach{
        case (cid, hc) => {
          val hotCategory1: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          val result: HotCategory = HotCategory(hotCategory1.cid, hotCategory1.clickCnt + hc.clickCnt, hotCategory1.orderCnt + hc.orderCnt, hotCategory1.payCnt + hc.payCnt)
          map1.update(cid,result)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = {
      hotCategory
    }
  }
}

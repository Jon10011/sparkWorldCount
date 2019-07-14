package com.songdong

import java.util

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义累加器
  */
class CustomerAccu extends AccumulatorV2[String, java.util.Set[String]] {

  var logStr = new util.HashSet[String]()


  //判断当前对象是否为空
  override def isZero: Boolean = logStr.isEmpty

  //复制对象
  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val accu = new CustomerAccu
    accu.logStr.addAll(logStr)
    accu
  }

  //重置对象
  override def reset(): Unit = logStr.clear()

  //分区内添加数据
  override def add(v: String): Unit = logStr.add(v)

  //分区间添加数据
  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit =
    logStr.addAll(other.value)

  //最终返回值
  override def value: util.Set[String] = logStr
}

/**
  * 累加器
  */
object Acc {
  def main(args: Array[String]): Unit = {
    //1、创建配置信息
    val conf = new SparkConf().setAppName("Jon").setMaster("local[*]")

    //2、创建spark context
    val sc = new SparkContext(conf)

    val arr = Array(1, 2, 3, 4, 5)

    //    val rdd = sc.makeRDD(arr)
    ////    var sum = 0
    //
    //    var sum = sc.accumulator(0) //加载累加器定义初始化值为0
    //
    //    //转换算子
    //    rdd.map { x =>
    //      sum += x
    //      sum
    //    }.collect()
    //
    //
    //    //    for (i <- arr) sum += i
    //
    //    println(sum)
    //

    val rdd = sc.makeRDD(Array("a", "b", "c"))

    val accu = new CustomerAccu

    sc.register(accu, "CustomerAccu")


    rdd.map { x =>
      accu.add(x)
      x
    }.collect()
    accu.value
    sc.stop()

  }

}


package com.songdong

import org.apache.spark.{SparkConf, SparkContext}

object Wordcount {
  def main(args: Array[String]): Unit = {

    //1、创建配置信息
    val conf = new SparkConf().setAppName("Jon")

    //2、创建spark context
    val sc = new SparkContext(conf)

    //3、处理

    //读取数据
    val lines = sc.textFile(args(0))

    //flatmap压平  扁平化
    val words = lines.flatMap(_.split(" "))

    //map(word,l)
    val k2v = words.map((_, 1))

    //reduceByKey(word,x)
    val result = k2v.reduceByKey(_ + _)

    //展示输出
    result.collect()

    //4、关闭连接
    sc.stop()

  }

}

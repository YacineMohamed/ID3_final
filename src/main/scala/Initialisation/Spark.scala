package Initialisation

import org.apache.spark.{SparkConf, SparkContext}

object Spark {
    val conf = new SparkConf()
      .setAppName("ID3")
      .setMaster("local[*]")
      //.set("spark.executor.memory","4g")

    val sc = new SparkContext(conf)
}
package com.scalaspark.exercises

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Parallize {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("parallize")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd:RDD[Int] = sc.parallelize(List(1,2,3,4,5))
    val rddcollect = rdd.collect()
    println("Number of Partition" +rdd.getNumPartitions)
    println("Action" +rdd.first())
    rddcollect.foreach(println)

  }

}

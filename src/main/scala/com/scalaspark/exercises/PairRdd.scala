package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object PairRdd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("pairRdd")
      .master("local[1]")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.parallelize(
      List("Germany India USA","USA India Russia","India Brazil Canada China")
    )
    val flatrdd = rdd.flatMap(_.split(" "))
    val pairRdd = flatrdd.map(f => (f , 1))
    pairRdd.foreach(println)

  }

}

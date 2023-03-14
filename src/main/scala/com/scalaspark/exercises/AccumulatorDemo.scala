package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Accumulator")
      .master("local[1]")
      .getOrCreate()

    val sc = spark.sparkContext;

    val longAcc = sc.longAccumulator("SumAccumulator")
    val rdd = sc.parallelize(Array(1,2,3,4))
    rdd.foreach(x => longAcc.add(x))

    println(longAcc.value)
    scala.io.StdIn.readLine()

  }
}

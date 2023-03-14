package com.scalaspark.exercises
import org.apache.spark.sql.SparkSession
object CreateRdd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("myFirstRDD")
      .master("local[1]")
      .getOrCreate()

    val rdd =spark.sparkContext.parallelize(Seq(("abhishek" , 1),("sam" , 2),("xyz" , 3)))


    rdd.foreach(println)
    scala.io.StdIn.readLine()

  }
}

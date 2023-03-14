package com.scalaspark.exercises
import org.apache.spark.sql.SparkSession
object ReadTextFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Reading Text file")
      .master("local[1]")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("data/textFile.txt")

    rdd.foreach(println)
//    scala.io.StdIn.readLine()

  }
}

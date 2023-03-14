package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object FlatMap {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DemoMap Method")
      .master("local[1]")
      .getOrCreate()

    val data = Seq("Project Gutenberg's",
      "Alice's Adventure in Wonderland",
      "project Gutenberge",
      "Adventure in Wonderland",
      "project gutenberg's")

    import spark.sqlContext.implicits._
    val df = data.toDF("data")
    df.show()

    val mapDF = df.map(fun => {
      fun.getString(0).split(" ")
    })
    mapDF.show()
  }

}

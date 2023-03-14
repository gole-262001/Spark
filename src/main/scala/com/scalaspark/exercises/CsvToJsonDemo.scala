package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object CsvToJsonDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("csvtojson")
      .master("local[1]")
      .getOrCreate()
      val dfFromCsv = spark.read.option("header",true)
        .csv("data/emp.csv")
    dfFromCsv.printSchema()
    dfFromCsv.write.json("data/simpleJson")

  }

}

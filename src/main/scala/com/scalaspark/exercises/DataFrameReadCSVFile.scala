package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object DataFrameReadCSVFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF Demo")
      .master("local[1]")
      .getOrCreate()
    val df = spark.read.option("header", true)
      .csv("data/emp.csv")
//    df.show()
//    df.select("name","Age").show()
    df.select("name", "Salary","Age","City").where("Age > 20").show()

  }
}

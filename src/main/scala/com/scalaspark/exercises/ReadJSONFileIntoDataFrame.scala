package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object ReadJSONFileIntoDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read JSON File into DataFrame")
      .master("local[1]")
      .getOrCreate()


    val multiline_df = spark.read.option("multiline" , true)
      .json("data/zipcodes.json")
    multiline_df.show(false)
  }

}

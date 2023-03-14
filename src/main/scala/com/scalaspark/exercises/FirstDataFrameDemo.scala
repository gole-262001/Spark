package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object FirstDataFrameDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF Demo")
      .master("local[1]")
      .getOrCreate()
    val data = Seq(("Anil","1000",29,"Hyderabadh"),
      ("Saksham","1000",20,"Guntur"),
      ("Nagaraj","10000",2,"Vizag"),
      ("Aditya","5000",23,"Anal"))

    val colms = Seq("name", "Salary", "Age", "City")
    import spark.sqlContext.implicits._

    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF(colms:_*)
    df.show()

  }


}

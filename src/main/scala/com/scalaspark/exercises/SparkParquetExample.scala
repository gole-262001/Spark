package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object SparkParquetExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Parquet Example")
      .master("local[1]")
      .getOrCreate()

    val data = Seq(("James ", "", "Smith", "36636", "M", 3000),
      ("Michael ", "Rose", "", "40288", "M", 4000),
      ("Robert ", "", "Williams", "42114", "M", 4000),
      ("Maria ", "Anne", "Jones", "39192", "F", 4000),
      ("Jen", "Mary", "Brown", "", "F", -1))
    val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
  
    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*)
    df.write.parquet("data/people.parquet")
    val parqdf = spark.read.parquet("data/people.parquet")
    parqdf.show();
  }

}

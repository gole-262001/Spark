package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object SparkSQLInnerJoinwithExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark SQL Inner Join with Example")
      .master("local[1]")
      .getOrCreate()

    val emp = Seq(
      (1, "Smith", -1, "2018", "10", "M", 3000)
    ,
    (2, "Rose", 1, "2010", "20", "M", 4000)
    ,
    (3, "Williams", 1, "2010", "10", "M", 1000)
    ,
    (4, "Jones", 2, "2005", "10", "F", 2000)
    ,
    (5, "Brown", 2, "2010", "40", "", -1)
    ,
    (6, "Brown", 2, "2010", "50", "", -1)
    )
    val empColumns = Seq("emp_id", "name", "superior_emp_id", "year_joined",
      "emp_dept_id", "gender", "salary")

    import spark.sqlContext.implicits._
    val empdf = emp.toDF(empColumns:_*)
    empdf.show(false)
    val dept = Seq(("Finance", 10),
      ("Marketing", 20),
      ("Sales", 30),
      ("IT", 40))

    val deptColumns = Seq("dept_name","dept_id")
    val deptdf = dept.toDF(deptColumns:_*)
    deptdf.show(false)

    empdf.join(deptdf,empdf("emp_dept_id") ===  deptdf("dept_id"),"inner").show(false)

  }

}

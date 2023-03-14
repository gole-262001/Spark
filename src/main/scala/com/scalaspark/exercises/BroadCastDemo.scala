package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object BroadCastDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("BroadCast Demo")
      .master("local[1]")
      .getOrCreate()

    val  inpuRdd = spark.sparkContext.parallelize(Seq(("Emp1" , "1000" ,"USA", "NY"),("Emp2" , "2000" ,"IND", "TS"),("Emp3" , "3000" ,"IND", "TN"),("Emp4" , "4000" ,"USA", "TX"),("Emp5" , "5000" ,"AUS", "QUE")))


    val countryData = Map(("USA" , "United States of America"),("IND" , "India"),("AUS" , "Australia"))


    val stateData = Map(("NY", "New York"),("TS", "Telangana"),("TN" ,"TamilNadu"),("TX","Texas"),("QUE","Queens"))


    val broadCastState = spark.sparkContext.broadcast(stateData)
    val broadCastCountries = spark.sparkContext.broadcast(countryData)

    val finalrdd = inpuRdd.map(f =>{
      val country = f._3
      val state = f._4
      val fullcountry = broadCastCountries.value.get(country).get
      val fullstate = broadCastState.value.get(state).get
      (f._1,f._2,fullcountry,fullstate)
    })

    println(finalrdd.collect().mkString("\n"))
    scala.io.StdIn.readLine() // see the web ui;





  }

}

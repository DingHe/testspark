package com.zzx.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object testsparksql {
  def main(args: Array[String]): Unit = {
      val conf=new SparkConf().setAppName("testsparksql").setMaster("local[4]")
      val sc=new SparkContext(conf)
      val sqlContext=new SQLContext(sc)
      val df=sqlContext.read.json("people.json")
//      df.show()
//      
//      df.printSchema()
//      
//      df.select("name").show()
//      df.select("age").show()
      
//       df.select(df("name"), df("age")+1).show()
//      
//       df.filter(df("age") > 29).show()
//       
//       df.groupBy("age").count().show()
      
   
      
  }
}
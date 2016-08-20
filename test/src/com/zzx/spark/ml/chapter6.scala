package com.zzx.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object chapter6 {
  def get_mapping(rdd:RDD[Array[String]],idx:Int) : Map[String,Long] = {
    
    rdd.map(_(idx)).distinct().zipWithIndex().collectAsMap()
    
  }
  
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[4]").setAppName("chapter6")
    val sc=new SparkContext(sparkconf)
    val rawData=sc.textFile("E:\\tmp\\Bike-Sharing-Dataset\\hour.csv")
    val num_data=rawData.count()
    println(num_data)
    val records=rawData.map { _.split(',') }
    println(records.first.mkString("\n"))
    
    println(get_mapping(records, 2).mkString("\n"))
    
   val mappings=for (i <- 2.to(10)) yield get_mapping(records, i)
   val cat_len=mappings.size
   val num_len=records.first().slice(10, 15).length
   val total_len=cat_len + num_len
   println(cat_len + ":" + num_len + ":" + total_len)
 
    
//    val mappings=for ( i <- 2.to(9))  
    
//    LinearRegressionWithSGD.tra
    
    
  }
  
}
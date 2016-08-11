package com.zzx.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object testsparkcore {
  def main(args: Array[String]): Unit = {
//       val conf=new SparkConf().setAppName("testsparkcore").setMaster(args(0))
      val conf=new SparkConf().setAppName("testsparkcore").setMaster("local[4]")
   
      val sc=new SparkContext(conf)
//      val data=Array(1,2,3,4,5)
//      val distData=sc.parallelize(data, 2)
//      val result=distData.reduce(_ + _)
//      System.out.println(result)
      
//        val distFile=sc.textFile("data.txt")
//        val result = distFile.flatMap(s => s.split(" ")).map((_,1))
//        val counts = result.reduceByKey(_ + _)
//        counts.foreach(println)
       
      
//       val wholefile=sc.wholeTextFiles("data.txt")
//       wholefile.foreach(println)
//       val foreachMap=wholefile.foreach(a => println(("filename="+a._1,"context="+a._2)))
       
//        val broadcastVar=sc.broadcast(Array(1,2,3))
//        broadcastVar.value.foreach { println}
      
        val accum=sc.accumulator(0,"My Accumulator")
        sc.parallelize(Array(1,2,3,4), 4).foreach { x => accum += x }
        println(accum.value)
       
       
       
  }
}
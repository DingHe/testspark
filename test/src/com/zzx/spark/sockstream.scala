package com.zzx.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object sockstream {
  
  def main(args: Array[String]): Unit = {
     val conf=new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
          
     val ssc=new StreamingContext(conf,Seconds(4))

     val lines=ssc.socketTextStream("120.27.24.47", 9999, StorageLevel.MEMORY_ONLY)
     val words=lines.flatMap(_.split(" ") )
     
     val pairs=words.map(word => (word,1))
     val wordCounts=pairs.reduceByKey(_ + _)
     wordCounts.print()
     
    
     ssc.start()
     ssc.awaitTermination()
     
     
  }
}
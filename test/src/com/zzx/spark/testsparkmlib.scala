package com.zzx.spark

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.immutable.Seq

object testsparkmlib {
  def main(args: Array[String]): Unit = {
    val dv:Vector=Vectors.dense(1.0, 0.0,3.0)
    println(dv.toJson)
    
    val sv1:Vector=Vectors.sparse(3, Array(0,2), Array(1.0,3.0))
    println(sv1.toJson)
    
    val sv2:Vector=Vectors.sparse(3, Seq((0,1.0),(2,3.0)))
    println(sv2.toJson)
        
  }
}
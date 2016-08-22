package com.zzx.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.clustering.KMeans
import breeze.linalg.DenseVector
import breeze.numerics.pow

object chapter7 {
  
 def computeDistance(v1:DenseVector[Double],v2:DenseVector[Double]) = pow(v1 - v2,2).sum
  
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[4]").setAppName("chapter4")
    val sc=new SparkContext(sparkconf)
    val movies=sc.textFile("E:\\tmp\\ml-100k\\u.item")
    println(movies.first())
    
    val genres=sc.textFile("E:\\tmp\\ml-100k\\u.genre")
     genres.take(5).foreach { println}
     
     val genreMap=genres.filter { x => !x.isEmpty() }.map{ line => line.split("\\|")}.map { x => 
       (x(1),x(0))  
     }.collectAsMap()
     
     println(genreMap.mkString("\n"))
     

     
     val titleAndGenres= movies.map { x => x.split("\\|") }.map { array => 
       val genres=array.toSeq.slice(5, array.size)
       val a= genres.zipWithIndex
       
       val genresAssigned=genres.zipWithIndex.filter{
         case (g,idx) => g == "1"
       }.map{ case (g,idx) => genreMap(idx.toString())}
       
       (array(0).toInt,(array(1),genresAssigned))
       
     }
     
     println(titleAndGenres.take(10).mkString("\n"))
     
     
     
      import org.apache.spark.mllib.recommendation.ALS
      import org.apache.spark.mllib.recommendation.Rating
      val rawData = sc.textFile("E:\\tmp\\ml-100k\\u.data")
      val rawRatings = rawData.map(_.split("\t").take(3))
      val ratings = rawRatings.map{ case Array(user, movie, rating) =>
      Rating(user.toInt, movie.toInt, rating.toDouble) }
      ratings.cache
      val alsModel = ALS.train(ratings, 50, 10, 0.1)
      
      
      val movieFactors=alsModel.productFeatures.map{ case (id,factor) => (id,Vectors.dense(factor))}
      val movieVectors=movieFactors.map{_._2}
      
      val userFactors=alsModel.userFeatures.map{case (id,factor) => (id,Vectors.dense(factor))}
      val userVectors=userFactors.map(_._2)
      
      val movieMatrix=new RowMatrix(movieVectors)
      val movieMatrixSummary=movieMatrix.computeColumnSummaryStatistics()
      
      val userMatrix=new RowMatrix(userVectors)
      val userMatrixSummary=userMatrix.computeColumnSummaryStatistics()
      
      println("Movie factors mean: "+movieMatrixSummary.mean)
      println("Movie factors variance: " + movieMatrixSummary.variance)
      println("User factors mean: " + userMatrixSummary.mean)
      println("User factors variance: " + userMatrixSummary.variance)  
      
      val numClusters=5
      val numIterations=10
      val numRuns=300
      
      val movieClusterModel=KMeans.train(movieVectors, numClusters, numIterations,numRuns)
      
      
      val userClusterModel = KMeans.train(userVectors, numClusters,numIterations, numRuns)
      
      val movie1=movieVectors.first()
      
      val movieCluster=movieClusterModel.predict(movie1)
      println(movieCluster)
      
      
      val predictions=movieClusterModel.predict(movieVectors)
      println(predictions.take(10).mkString("\n"))
      
      
      
      val titlesWithFactors=titleAndGenres.join(movieFactors)
      
      val movieAssgined=titlesWithFactors.map{case (id,((title,genres),vecotr)) =>
        val pred=movieClusterModel.predict(vecotr)
        val clusterCentre=movieClusterModel.clusterCenters(pred)
        val dist=computeDistance(DenseVector(clusterCentre.toArray), DenseVector(vecotr.toArray))
        (id,title,genres.mkString(" "),pred,dist)
        
      }
      
      val clusterAssignments=movieAssgined.groupBy{case (id,title,genres,cluster,dist) => cluster}.collectAsMap
      
      for((k,v) <- clusterAssignments.toSeq.sortBy(_._1)){
        println(s"Cluster $k:")
        val m=v.toSeq.sortBy(_._5)
        println(m.take(20).map{case (_,title,genres,_,d) => (title,genres,d)}.mkString("\n"))
        println("==========\n")
      }
      
  }
}
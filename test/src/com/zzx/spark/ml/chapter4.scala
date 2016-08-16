package com.zzx.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.jblas.DoubleMatrix



object chapter4 {
  
  //计算两个矩阵的余弦相似度
  def consineSimilarity(vec1:DoubleMatrix,vec2:DoubleMatrix):Double ={
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
  
  def avgPrecisionK(actual:Seq[Int],predicted:Seq[Int],k:Int):Double={
    val predK=predicted.take(k)
    var score=0.0
    var numHits=0.0
    for((p,i) <- predK.zipWithIndex){
      if(actual.contains(p)){
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if(actual.isEmpty){
      1.0
    }else{
      score / math.min(actual.size, k).toDouble
    }
  }
  
  
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[4]").setAppName("chapter4")
    val sc=new SparkContext(sparkconf)
    val rawData=sc.textFile("E:\\tmp\\ml-100k\\u.data")
//    println(rawData.first())
    
     val rawRatings=rawData.map(_.split("\t").take(3))
//    rawRatings.first().foreach { println }
    //映射为评分RDD
     val ratings=rawRatings.map{ case Array(user,movie,rating) => Rating(user.toInt,movie.toInt,rating.toDouble)}
//    println(ratings.first.toString())
     val model=ALS.train(ratings, 50, 10, 0.01)
     println(model.userFeatures.count())
     println(model.productFeatures.count())
     
     val predictedRating=model.predict(789, 123)
     println(predictedRating)
     
     val topKRecs=model.recommendProducts(789, 10)
     topKRecs.foreach { println }
     
     
     //inspecting the recommendations
     val movies=sc.textFile("E:\\tmp\\ml-100k\\u.item")
     val titles=movies.map { line => line.split('|').take(2) }.map { array => (array(0).toInt,array(1)) }.collectAsMap()
     println(titles(123))
    
     
     val moviesForUser=ratings.keyBy { _.user }.lookup(789)
     println(moviesForUser.size)
     
     
     moviesForUser.sortBy {- _.rating }.take(10).map(rating => (titles(rating.product),rating.product,rating.rating)).foreach(println)
     
     
     
     
//     val aMatrix=new DoubleMatrix(Array(1.0,2.0,3.0))
//     println(aMatrix.toString())
     
     
     
    val itemId=567
    val itemFactor=model.productFeatures.lookup(itemId).head
    
    val itemVector=new DoubleMatrix(itemFactor)
    println(itemVector.toString())
    println(consineSimilarity(itemVector, itemVector))
    
    
    val sims=model.productFeatures.map{ case (id,factor) => 
        val factorVector=new DoubleMatrix(factor)
        val sim=consineSimilarity(factorVector, itemVector)
        (id,sim)
    }
    
    
   println(sims.take(10).mkString("\n"))
     
     
    val sortedSims=sims.top(11)(Ordering.by[(Int,Double),Double] { case
      (id,similarity) => similarity
    })
    
    
    println(sortedSims.take(10).mkString("\n"))
    println(titles(itemId))
    
   val top10= sortedSims.slice(1, 11).map{ case (id,sim) => (titles(id),sim)}
   println( top10.mkString("\n"))
   
   
   //均方差
   val actualRating=moviesForUser.take(1)(0)
   println(actualRating.toString())
   
   val predictedRating2=model.predict(actualRating.user, actualRating.product)
   println(predictedRating2)
   
   val squareError=math.pow(predictedRating2 - actualRating.rating, 2.0)
   println(squareError)
   
   println("==================================")
   val userProducts=ratings.map{ case Rating(user,product,rating) => (user,product)}
   val predictions=model.predict(userProducts).map{case Rating(user,product,rating) => ((user,product),rating)}
   
   val ratingsAndPredictions=ratings.map{ case Rating(user,product,rating) => ((user,product),rating)}
   
   
   val result=ratingsAndPredictions.join(predictions)
   
   val MSE=result.map{ case((user,product),(actual,predicted)) => math.pow((actual-predicted), 2.0)}.reduce(_ + _) / result.count()
   
   println(MSE)
   
   
   val actualMovies=moviesForUser.map(_.product)
   actualMovies.foreach { println }
   
   val predictedMovies=topKRecs.map(_.product)
   
   val apk10=avgPrecisionK(actualMovies, predictedMovies, 10)
   println("apk10="+apk10)
   
   
   val itemFactors=model.productFeatures.map{ case (id,factor) => factor}.collect()
   val itemMatrix=new DoubleMatrix(itemFactors)
   println(itemMatrix.rows,itemMatrix.columns)
   
   val imBroadcase=sc.broadcast(itemMatrix)
   
   val allRecs=model.userFeatures.map{ case(userId,array) => 
     val userVector=new DoubleMatrix(array)
     val scores=imBroadcase.value.mmul(userVector)
     val sortedWithId=scores.data.zipWithIndex.sortBy(-_._1)
     val recommendedIds=sortedWithId.map(_._2 + 1).toSeq
     
     (userId,recommendedIds)
     
   }
   
   
   
//   val predictedAndTrue=ratingsAndPredictions.map{ case((user,product),(predicted,actual)) => (predicted,actual)}
   
  }
}

















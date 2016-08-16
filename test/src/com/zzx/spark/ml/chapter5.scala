package com.zzx.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.classification.SVMModel

object chapter5 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[4]").setAppName("chapter5")
    val   sc=new SparkContext(conf)
    val rawData=sc.textFile("E:\\tmp\\train.tsv")
    val records=rawData.map(line => line.split("\t"))
    println(records.first().mkString("\n"))
    
    val data=records.map{ r => 
      val trimmed=r.map(_.replaceAll("\"", ""))
      val label=trimmed(r.size -1).toInt
      val features=trimmed.slice(4, r.size-1).map { d => if(d == "?") 0.0 else d.toDouble }
      LabeledPoint(label,Vectors.dense(features))
    }
    
    val numData=data.count()
    println(numData)
    
    
    val nbData=records.map{ r => 
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label=trimmed(r.size -1).toInt
      val features=trimmed.slice(4, r.size - 1).map(d => if(d == "?") 0.0 else d.toDouble).map( d => 
        if(d < 0) 0.0 else d)
       LabeledPoint(label,Vectors.dense(features))
    }
    
    
    
    val numInterations=10
    val maxTreeDepth=5
    val lrModel=LogisticRegressionWithSGD.train(data, numInterations)
    
    val svmModel=SVMWithSGD.train(data, numInterations)
    
    
    val nbModel=NaiveBayes.train(nbData)
    
//    val dtModel=DecisionTree.train(data,Classification,Entropy, maxTreeDepth)
//    
    
    //模型预测检验
    val dataPoint=data.first()
    val prediction=lrModel.predict(dataPoint.features)
    println(prediction)
    
    println(dataPoint.label)
    
    val lrTotalCorrect=data.map{ point => 
       if(lrModel.predict(point.features) == point.label) 1 else 0
    }.sum()
    
    
    val lrAccuracy=lrTotalCorrect/data.count()
    println(lrAccuracy)
    
    
    val predictResult=data.map{point => lrModel.predict(point.features)}
    println(predictResult.collect().mkString("\n"))
    
    
    val svmTotalCorrect=data.map{point => if(svmModel.predict(point.features) == point.label) 1 else 0}.sum()
    println(svmTotalCorrect/numData)
    
    val nbTotalCorrect=nbData.map{point => if(nbModel.predict(point.features) == point.label) 1 else 0}.sum()
    println(nbTotalCorrect/numData)
    

    
    
  }
}
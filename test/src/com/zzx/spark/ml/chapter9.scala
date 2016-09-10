package com.zzx.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.matching.Regex
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import breeze.linalg._



object chapter9 { 
  
  def tokenize(line: String): Seq[String] = {
    val regex="""[^0-9]*""".r
    val stopwords  = Set("the","a","an","of","or","in","for","by","on","but", "is", "not","with", "as", "was", "if","they", "are", "this", "and", "it", "have", "from", "at", "my","be", "that", "to")
   
    line.split("""\W+""").map(_.toLowerCase).filter(token => regex.pattern.matcher(token).matches).filterNot(token => stopwords.contains(token)).filter(token => token.size >= 2).toSeq
  }
  
  
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[4]").setAppName("chapter9")
    
    val sc=new SparkContext(conf)
    
    val conf2=sc.getConf
    conf2.getAll.foreach(println)
    
    
    val path="/path/20news-bydate-train/*"
    val rdd=sc.wholeTextFiles(path,10)
    rdd.getNumPartitions
    
    rdd.cache()
    val file=rdd.map{case(file,text) => file}
    println(file.count())
    
    val newsgroups=rdd.map{
      case(file,text) => file.split('/').takeRight(2).head
    }
    
    newsgroups.cache()
    
    newsgroups.collect().foreach { println }
    
    val countByGroup=newsgroups.map(n => (n,1)).reduceByKey(_ + _).collect.sortBy(-_._2).mkString("\n")
    
    
    val text=rdd.map{case (file,text) => text}
    text.cache()
    
    val whiteSpaceSplit=text.flatMap { t => t.split(" ").map(_.toLowerCase()) }
    println(whiteSpaceSplit.distinct().count())
    
    
    println(whiteSpaceSplit.sample(true, 0.3, 42).take(100).mkString(","))
    
    
    val nonWordSplit=text.flatMap { t => t.split("""\W+""").map(_.toLowerCase) }
    println(nonWordSplit.distinct().count())
    
    println(nonWordSplit.distinct().sample(true, 0.3, 42).take(1000).mkString(","))
    
   val regex="""[^0-9]*""".r
    val filterNumbers=nonWordSplit.filter { token => regex.pattern.matcher(token).matches() }
    println(filterNumbers.distinct().count())
    
    println(filterNumbers.distinct().sample(true, 0.3, 42).take(1000).mkString(","))
    
    val tokenCounts=filterNumbers.map{t => (t,1)}.reduceByKey(_ + _)
    
    val oreringDesc=Ordering.by[(String,Int),Int](_._2)    //其中(String,Int)是输入参数,Int是输出参数,返回值就是需要比较的内容
    
    println(tokenCounts.top(20)(oreringDesc).mkString("\n"))
    
    
   val stopwords  = Set("the","a","an","of","or","in","for","by","on","but", "is", "not","with", "as", "was", "if","they", "are", "this", "and", "it", "have", "from", "at", "my","be", "that", "to")
    val tokenCountsFilteredStopWords=tokenCounts.filter{case (k,v) => !stopwords.contains(k)}
    println(tokenCountsFilteredStopWords.top(20)(oreringDesc).mkString("\n"))
    
    val tokenCountsFilteredSize=tokenCountsFilteredStopWords.filter{case (k,v) => k.size >=2}
    println(tokenCountsFilteredSize.top(20)(oreringDesc).mkString("\n"))
    
    
    val orderingAsc=Ordering.by[(String,Int),Int](-_._2)
    println(tokenCountsFilteredSize.top(20)(orderingAsc).mkString("\n"))
    
    val rareTokens=tokenCounts.filter{case (k,v) => v < 2}.map{case (k,v) => k}.collect().toSet
    println(rareTokens.mkString(","))
    
    val tokenCountsFilteredAll=tokenCountsFilteredSize.filter{case (k,v) => ! rareTokens.contains(k)}
    println(tokenCountsFilteredAll.top(20)(orderingAsc).mkString("\n"))
    println(tokenCountsFilteredAll.count())
    
    
    tokenCountsFilteredAll.take(2).foreach(println)
    
    
    
    
    println(text.flatMap { doc => tokenize(doc) }.distinct.count)
    val tokens=text.map{doc => tokenize(doc)}
    println(tokens.first().take(20))
    
    val dim=math.pow(2, 18).toInt
    val hashingTF=new HashingTF(dim)
    val tf=hashingTF.transform(tokens)
    tf.cache()
    
    
    val v=tf.first.asInstanceOf[SV]
    println(v.size)
    println(v.values.size)
    println(v.values.take(10).toSeq)
    println(v.indices.take(10).toSeq)
    
    val idf=new IDF().fit(tf)
    val tfidf=idf.transform(tf)
    val v2=tfidf.first().asInstanceOf[SV]
    println(v2.values.size)
    println(v2.values.take(10).toSeq)
    println(v2.indices.take(10).toSeq)
    
    
    val minMaxVals=tfidf.map { v => 
      val sv=v.asInstanceOf[SV]
      (sv.values.min,sv.values.max)
    }
    
    val globalMinMax=minMaxVals.reduce{case ((min1,max1),(min2,max2)) =>
      (math.min(min1, min2),math.max(max1, max2))  
    }
    
    println(globalMinMax)
    
    
    //val common=sc.parallelize(Seq("you","do","we"))
    val common=sc.parallelize(Seq(Seq("you","do","we")))
    val tfCommon=hashingTF.transform(common)
    val tfidfCommon=idf.transform(tfCommon)
    val commonVector=tfidfCommon.first().asInstanceOf[SV]
    println(commonVector.values.toSeq)
    
    
    val uncommon = sc.parallelize(Seq(Seq("telescope", "legislation","investment")))
    val tfUncommon = hashingTF.transform(uncommon)
    val tfidfUncommon = idf.transform(tfUncommon)
    val uncommonVector = tfidfUncommon.first.asInstanceOf[SV]
    println(uncommonVector.values.toSeq)
    
    
    val hockeyText=rdd.filter{case (file,text) => file.contains("hockey")}
    val hockeyTF=hockeyText.mapValues { doc => hashingTF.transform(tokenize(doc)) }
    val hockeyTfIdf=idf.transform(hockeyTF.map(_._2))
    
    
    val hockey1=hockeyTfIdf.sample(true, 0.1, 42).first().asInstanceOf[SV]
    val breeze1=new SparseVector(hockey1.indices,hockey1.values,hockey1.size)  
    
    
    val hockey2=hockeyTfIdf.sample(true, 0.1, 43).first().asInstanceOf[SV]
    val breeze2=new SparseVector(hockey2.indices,hockey2.values,hockey2.size)
    
    
    val cosineSim=breeze1.dot(breeze2) / (norm(breeze1) * norm(breeze2))
    println(cosineSim)
    
    
    
    
    
    
    
    
    
    
    
  }
}
package com.zzx.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object chapter9 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[4]").setAppName("chapter9")
    val sc=new SparkContext(conf)
    
    val path="/path/20news-bydate-train/*"
    val rdd=sc.wholeTextFiles(path)
    val file=rdd.map{case(file,text) => file}
    println(file.count())
    
    val newsgroups=rdd.map{
      case(file,text) => file.split('/').takeRight(2).head
    }
    
    newsgroups.collect().foreach { println }
    
    val countByGroup=newsgroups.map(n => (n,1)).reduceByKey(_ + _).collect.sortBy(-_._2).mkString("\n")
    println(countByGroup)
    
    val text=rdd.map{case (file,text) => text}
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
    
    
    val stopwords = Set("the","a","an","of","or","in","for","by","on","but", "is", "not","with", "as", "was", "if","they", "are", "this", "and", "it", "have", "from", "at", "my","be", "that", "to")
    
    val tokenCountsFilteredStopWords=tokenCounts.filter{case (k,v) => !stopwords.contains(k)}
    println(tokenCountsFilteredStopWords.top(20)(oreringDesc).mkString("\n"))
    
    val tokenCountsFilteredSize=tokenCountsFilteredStopWords.filter{case (k,v) => k.size >=2}
      println(tokenCountsFilteredSize.top(20)(oreringDesc).mkString("\n"))
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
  }
}
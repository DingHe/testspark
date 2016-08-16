package com.zzx.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object chapter3 {
  
  def convert_year(x:String) :String={
    try{
      x.substring(x.length()-4)
    }catch{
      case _:Exception => "1990" 
    }
  }
  
  def main(args: Array[String]): Unit = {
    
    //探索用户数据
    val sc=new SparkContext(new SparkConf().setMaster("local[4]").setAppName("chapter3"))
//    val user_data=sc.textFile("/path/ml-100k/u.user")
    val user_data=sc.textFile("E:\\tmp\\ml-100k\\u.user")
    
    user_data.take(10).foreach(println)
    val user_fields=user_data.map(_.split('|'))   //这里特别注意，"|"分解不出来，只能用'|'
    
    user_fields.first().foreach(println)
    
    
    
    val num_users=user_fields.map(a => a(0)).count()
    println(num_users)
    val num_genders=user_fields.map(a => a(2)).distinct().count()
    
    val num_occupations=user_fields.map(_(3)).distinct().count()
    println(num_occupations)
    
    val num_zipcodes=user_fields.map(_(4)).distinct().count()
    
    printf("Users: %d, genders: %d, occupations: %d, ZIP codes: %d",num_users, num_genders, num_occupations, num_zipcodes)
    
    
    val count_by_occupation=user_fields.map(x => (x(3),1)).reduceByKey(_+_).sortBy(_._2, false).collect()
    count_by_occupation.foreach(println)
    
    
    //探索电影数据
    
//    val movie_data=sc.textFile("/path/ml-100k/u.item")
    val movie_data=sc.textFile("E:\\tmp\\ml-100k\\u.item")
    println(movie_data.first())
    println(movie_data.count())
    val movie_fields=movie_data.map(_.split('|'))   //这里特别注意，"|"分解不出来，只能用'|'
    movie_fields.first().foreach { println }
    println(convert_year(movie_fields.first()(2)))
    
    val years=movie_fields.map(x => convert_year((x(2))))
    (years.take(100)).foreach(println)
    
    val years_filtered=years.filter { x => x != "1990" }
     (years_filtered.take(100)).foreach(println)
     
    val movie_ages=years_filtered.map( 1998 - _.toInt).map((_,1L)).reduceByKey(_ + _)
    movie_ages.collect().sortBy(_._2).foreach(println)
    
    
    
   //探索评分数据
//    val rating_data=sc.textFile("/path/ml-100k/u.data")
     val rating_data_raw=sc.textFile("E:\\tmp\\ml-100k\\u.data")
     println(rating_data_raw.first())
     val num_ratings=rating_data_raw.count()
     println(num_ratings)
     
     val rating_data=rating_data_raw.map(_.split("\t"))
     rating_data.first().foreach(println)
     val ratings=rating_data.map(_(2).toInt)
     ratings.take(10).foreach(println)
     val max_rating=ratings.reduce((x,y) => x.max(y))
     println(max_rating)
     
     val min_rating=ratings.reduce((x,y) => x.min(y))
     println(min_rating)
     
     val mean_rating=ratings.reduce((x,y) => x+y)/num_ratings
     println(mean_rating)
     
    println( ratings.stats())
    
    
   
    
  }
}
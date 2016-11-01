package com.zzx.md5

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.security.MessageDigest


object ProccessPhoneNum {
  
  
  /*
   * 可以和下面linux加密结果对比是否加密后正确
   * echo -n "18918910000" |  md5sum|cut -d ' ' -f1
   */
  def EncoderByMD5(str:String):String = {
    val md5=MessageDigest.getInstance("MD5")
    md5.digest(str.getBytes).map("%02x".format(_)).mkString
  }
  
  def main(args: Array[String]): Unit = {
    //spark-shell --master yarn --name ProccessPhoneNum --driver-memory 2g --executor-memory 1g  --executor-cores 1  --num-executors 20
    
    
    val path="/user/hive/warehouse/hnlyw.db/t_result/"
    val sc=new SparkContext(new SparkConf().setMaster("local[4]").setAppName("ProccessPhoneNum"))
    val phone_rdd=sc.textFile(path)
    val fields_rdd=phone_rdd.map(_.split('\001'))
    //fields_rdd.first().foreach(println)
    val result_rdd=fields_rdd.map { case Array(phone,md5_phone,province,city) =>  phone +","+ md5_phone+","+EncoderByMD5(md5_phone)+","+province+","+city}
    result_rdd.first()
    result_rdd.saveAsTextFile("/user/hive/warehouse/hnlyw.db/t_result2/")
    
   
    
  }
}
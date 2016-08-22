package com.zzx.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import breeze.linalg.DenseMatrix

object chapter8 {
  
  def loadImageFromFile(path:String) : BufferedImage = {
//    ImageIO.read(new File(path))
    val conf=new Configuration()
    conf.set("fs.default.name","hdfs://iZ28sjghw68Z:8020")
    val fs=FileSystem.get(conf)
    val in=fs.open(new Path(URI.create(path)))
    ImageIO.read(in)
  }
  
  
  def processImage(image:BufferedImage,width:Int,height:Int) : BufferedImage ={
    val bwImage=new BufferedImage(width,height,BufferedImage.TYPE_BYTE_GRAY)
    val g=bwImage.getGraphics()
    g.drawImage(image, 0, 0, width, height,null)
    g.dispose()
    bwImage
  }
  
  def getPixelsFromImage(image:BufferedImage):Array[Double] ={
    val width=image.getWidth
    val height=image.getHeight
    val pixels=Array.ofDim[Double](width * height)
    image.getData.getPixels(0, 0, width, height, pixels)
    
  }
  
  def extractPixels(path:String,width:Int,height:Int):Array[Double] ={
    val raw=loadImageFromFile(path)
    val processed=processImage(raw, width, height)
    getPixelsFromImage(processed)
  }
  
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[4]").setAppName("chapter8")
    val sc=new SparkContext(sparkconf)
//    val rdd=sc.wholeTextFiles("E:\\tmp\\lfw-a\\lfw\\*")
    val rdd=sc.wholeTextFiles("/path/lfw/*")
    
    println(rdd.count())
    val first=rdd.first()
    println(first)
    
//    val files=rdd.map{ case (fileName,content) =>          //针对本地模式跑的程序，去掉了file:符号
//      fileName.replace("file:", "")
//    }
//    println(files.first())
    val files=rdd.map{case(fileName,content) => fileName}
    files.collect().foreach { println }
    
    
    
    val aePath = files.first()
    val aeImage = loadImageFromFile(aePath)
    
    val grayImage = processImage(aeImage, 100, 100)
    
    
    val pixels=files.map { f => extractPixels(f, 50, 50) }
    
    println(pixels.take(10).map { _.take(10).mkString("" , "," , ",...") }.mkString("\n"))
    println(pixels.take(10).map { f => f.mkString(",") }.mkString("\n"))
    
    val vectors=pixels.map { p => Vectors.dense(p) }
    vectors.setName("Image-vectors")
    vectors.cache()
    
    
    val scaler=new StandardScaler(withMean=true,withStd=false).fit(vectors)
    
    val scaledVectors=vectors.map(v => scaler.transform(v))
    
    val matrix=new RowMatrix(scaledVectors)
    val k=10
    val pc=matrix.computePrincipalComponents(k)
    
    val rows=pc.numRows
    val cols=pc.numCols
    println(rows,cols)
    
    val pcBreeze=new DenseMatrix(rows,cols,pc.toArray)
    import breeze.linalg.csvwrite
    csvwrite(new File("/tmp/pc.csv"), pcBreeze)  //can't run on  hdfs
    
    val projected=matrix.multiply(pc)
    println(projected.numRows(),projected.numCols())
    
    println(projected.rows.take(5).mkString("\n"))
    
    val svd=matrix.computeSVD(k, computeU=true)
    println(s"U dimension:(${svd.U.numRows()},${svd.U.numCols()})")
    println(s"S dimension:(${svd.s.size},)")
    println(s"V dimension:(${svd.V.numRows},${svd.V.numCols})")
    
  }
}

























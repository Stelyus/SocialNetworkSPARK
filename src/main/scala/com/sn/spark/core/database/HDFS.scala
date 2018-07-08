package com.sn.spark.core.database


import com.datastax.spark.connector._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.util.Date

import com.sn.spark.core.api.model.Response.{MessageResponseObject, PostResponseObject}
import org.apache.spark.rdd.RDD


object HDFS{
  val hdfs = "hdfs://localhost:9000"
  val messagePath = "/data/HDFS_message"
  val postPath = "/data/HDFS_post"

  def searchBetweenTwoDate(dateFrom : Date, dateTo : Date, Brand: String): (RDD[MessageResponseObject.MessageResponse], RDD[PostResponseObject.PostResponse]) ={
    val rdd = readHDFS(hdfs + messagePath)
    val rdd2 = readHDFS(hdfs + postPath)

    val messageRes = rdd.filter(x => dateFrom.before(x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).asInstanceOf[Date]) &&
      dateTo.after(x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).asInstanceOf[Date]) &&
      x.columnValues(x.metaData.namesToIndex.getOrElse("text", 0)).toString.toLowerCase.contains(Brand.toLowerCase)).map(x => MessageResponseObject.toMessageResponse((x)))

    val postRes = rdd2.filter(x => dateFrom.before(x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).asInstanceOf[Date]) &&
      dateTo.after(x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).asInstanceOf[Date]) &&
      x.columnValues(x.metaData.namesToIndex.getOrElse("text", 0)).toString.toLowerCase.contains(Brand.toLowerCase)).map(x => PostResponseObject.toPostResponse((x)))

    (messageRes, postRes)
  }

  def searchAfterDate(dateFrom : Date, Brand: String): (RDD[MessageResponseObject.MessageResponse], RDD[PostResponseObject.PostResponse]) ={
    val rdd = readHDFS(hdfs + messagePath)
    val rdd2 = readHDFS(hdfs + postPath)

    val messageRes = rdd.filter(x => dateFrom.before(x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).asInstanceOf[Date]) &&
      x.columnValues(x.metaData.namesToIndex.getOrElse("text", 0)).toString.toLowerCase.contains(Brand.toLowerCase)).map(x => MessageResponseObject.toMessageResponse((x)))

    val postRes = rdd2.filter(x => dateFrom.before(x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).asInstanceOf[Date]) &&
      x.columnValues(x.metaData.namesToIndex.getOrElse("text", 0)).toString.toLowerCase.contains(Brand.toLowerCase)).map(x => PostResponseObject.toPostResponse((x)))

    (messageRes, postRes)
  }

  def searchBeforeDate(dateTo : Date, Brand: String): (RDD[MessageResponseObject.MessageResponse], RDD[PostResponseObject.PostResponse]) ={
    val rdd = readHDFS(hdfs + messagePath)
    val rdd2 = readHDFS(hdfs + postPath)

    val messageRes = rdd.filter(x => dateTo.after(x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).asInstanceOf[Date]) &&
      x.columnValues(x.metaData.namesToIndex.getOrElse("text", 0)).toString.toLowerCase.contains(Brand.toLowerCase)).map(x => MessageResponseObject.toMessageResponse((x)))

    val postRes = rdd2.filter(x => dateTo.after(x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).asInstanceOf[Date]) &&
      x.columnValues(x.metaData.namesToIndex.getOrElse("text", 0)).toString.toLowerCase.contains(Brand.toLowerCase)).map(x => PostResponseObject.toPostResponse((x)))

    (messageRes, postRes)
  }

  def searchForever(Brand: String): (RDD[MessageResponseObject.MessageResponse], RDD[PostResponseObject.PostResponse]) ={
    val rdd = readHDFS(hdfs + messagePath)
    val rdd2 = readHDFS(hdfs + postPath)

    val messageRes = rdd.filter(x => x.columnValues(x.metaData.namesToIndex.getOrElse("text", 0)).toString.toLowerCase.contains(Brand.toLowerCase)).map(x => MessageResponseObject.toMessageResponse((x)))

    val postRes = rdd2.filter(x => x.columnValues(x.metaData.namesToIndex.getOrElse("text", 0)).toString.toLowerCase.contains(Brand.toLowerCase)).map(x => PostResponseObject.toPostResponse((x)))

    (messageRes, postRes)
  }

  def saveToFile(path: String, base: String, table: String): Unit ={
    val fs = FileSystem.get(new java.net.URI(hdfs), Cassandra.sc.hadoopConfiguration)
    if(fs.exists(new org.apache.hadoop.fs.Path(path))) {
      val save = readHDFS(hdfs + path)
      save.saveToCassandra(base, table)

      fs.delete(new org.apache.hadoop.fs.Path(path), true)
      val rdd = Cassandra.sc.cassandraTable(base, table)
      rdd.saveAsObjectFile(hdfs + path)
    }
    else{
      val rdd = Cassandra.sc.cassandraTable(base, table)
      rdd.saveAsObjectFile(hdfs + path)
    }
  }

  def saveAllHDFS(base: String): Unit={
    saveToFile("/data/HDFS_user", base, "user")
    saveToFile("/data/HDFS_message", base, "message")
    saveToFile("/data/HDFS_like", base, "like")
    saveToFile("/data/HDFS_location", base, "location")
    saveToFile("/data/HDFS_post", base, "post")
  }

  def readHDFS(path: String): RDD[CassandraRow] = {
    Cassandra.sc.objectFile(path)
  }

  def script(): Unit ={
    saveAllHDFS("spark")
  }
}

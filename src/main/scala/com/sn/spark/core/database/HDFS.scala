package com.sn.spark.core.database

import java.time.Instant

import com.datastax.spark.connector._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.util.Date

import com.sn.spark.core.api.model.Response.{MessageResponseObject, PostResponseObject}
import org.apache.spark.rdd.RDD
import com.sn.spark.core.model._


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
      println("TROUVEEEEEEEEEEEEEEEEEEEEEEEEEE")

      println("old hdfs")
      val save = readHDFS(hdfs + path)
      save.foreach(println)

      fs.delete(new org.apache.hadoop.fs.Path(path), true)

//      save.saveToCassandra(base, table)

      println("cass")
      val rdd = Cassandra.sc.cassandraTable(base, table)
      rdd.foreach(println)

//      println("new")
//      val newHDFS = rdd.union(save)
//      newHDFS.foreach(println)
//
//      println("java rdd")
//      val java = save.toJavaRDD().union(rdd.toJavaRDD())
//      java.rdd.foreach(println)

      rdd.saveAsObjectFile(hdfs + path)
    }
    else{
      println("PAS TROUVER")
      val rdd = Cassandra.sc.cassandraTable(base, table)
      rdd.saveAsObjectFile(hdfs + path)
    }

  }

  def saveAllHDFS(base: String): Unit={
   //saveToFile("/data/HDFS_user", base, "user")
    saveToFile("/data/HDFS_message", base, "message")
   // saveToFile("/data/HDFS_like", base, "like")
   // saveToFile("/data/HDFS_location", base, "location")
    saveToFile("/data/HDFS_post", base, "post")
  }

  def readHDFS(path: String): RDD[CassandraRow] = {
    Cassandra.sc.objectFile(path)
  }

  def script(): Unit ={
    val userPath = "/data/HDFS_user"

    val usr = new User("jean", "bernard", "jojo3@gmail.com", "jojo", Instant.now(), false)
    val message = new Message(Id("b1f70bd0-7fa1-11e2-a9f9-2f02517cf2f9"), Instant.now(), Id[User]("jojo3@gmail.com"), Id[User]("jojo@gmail.com"), "Auchan j aime pas", false)
    val post = new Post(Id("b1f70be0-7fa1-11e9-a9f9-2f02517be4f7"),Instant.now(),  Id[User]("jojo3@gmail.com"), "je suis a Auchan")

//    Cassandra.sendMessage(message)
//    Cassandra.sendPost(post)
    saveAllHDFS("spark")

    println("Printing hdfs new value:")
    readHDFS(hdfs + messagePath).foreach(println)
    readHDFS(hdfs + postPath).foreach(println)

//    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
//    val t = searchAfterDate(format.parse("2018-07-06"), "Auchan")
//    t._1.foreach(println)
//    t._2.foreach(println)
  }
}

package com.sn.spark.core.database

import java.time.Instant
import com.datastax.spark.connector._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.util.Date
import org.apache.spark.rdd.RDD
import com.sn.spark.core.model._


object HDFS{
  val hdfs = "hdfs://localhost:9000"

  def saveToFile(path: String, base: String, table: String): Unit ={
    val fs = FileSystem.get(new java.net.URI(hdfs), Cassandra.sc.hadoopConfiguration)
    if(fs.exists(new Path(path)))
      fs.delete(new Path(path),true)

    val rdd = Cassandra.sc.cassandraTable(base, table)
    rdd.saveAsObjectFile(hdfs + path)
  }
  def search(dateFrom : Date, dateTo : Date, rdd: RDD[CassandraRow]): Unit ={
    rdd.foreach(x =>
      if (dateFrom.before(x.columnValues(2).asInstanceOf[Date]) && dateTo.after(x.columnValues(2).asInstanceOf[Date])) {
        if (x.contains("Auchan")) {
          println("yes" + x)
        }
      } else {
        println("nope: " + x)
      })
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
    val userPath = "/data/HDFS_user"
    val messagePath = "/data/HDFS_message"
    val usr = new User("jean", "bernard", "jojo3@gmail.com", "jojo", Instant.now(), false)
    val message = new Message(Id("b1f70be0-7fa1-11e8-a9f9-2f02517be4d5"), Instant.now(), Id[User]("jojo3@gmail.com"), Id[User]("jojo@gmail.com"), "je suis a Burger king moi", false)

    //Cassandra.sendMessage(message)
    //saveAllHDFS("spark")
    val rdd = readHDFS(hdfs + messagePath)//filter(_.contains("Auchan"))foreach(println)
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    search(format.parse("2018-07-06"), format.parse("2018-07-08"), rdd)
  }
}

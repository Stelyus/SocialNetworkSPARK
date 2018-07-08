import java.time.Instant

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.WriteConf
import org.apache.hadoop.fs.{FileSystem, Path}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.rdd.RDD
import com.sn.spark.Topic
import com.sn.spark.core.consumer._
import org.apache.spark._
import com.sn.spark.core.model._
import com.sn.spark.core.producer._
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.JavaConversions._

object HDFS{
  val hdfs = "hdfs://localhost:9000"

  def saveToFile(path: String, base: String, table: String): Unit ={
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfs), sc.hadoopConfiguration)
    if(fs.exists(new org.apache.hadoop.fs.Path(path)))
      fs.delete(new org.apache.hadoop.fs.Path(path),true)

    val rdd = Cassandra.sc.cassandraTable(base, table)
    rdd.saveAsObjectFile(hdfs + path)
  }
  def search(time : Date, rdd: RDD[CassandraRow]): Unit ={
    rdd.foreach(x =>
      if (time.before(x.columnValues(1).asInstanceOf[Date])) {
        println(x)
      } else {
        println("nope")
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
    val rdd = readHDFS(hdfs + userPath)//filter(_.contains("Auchan"))foreach(println)
    search(new Date(2017, 8, 8, 19,26, 30), rdd)
  }
}

package com.sn.spark.core.database
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

object Cassandra {

  val conf = new SparkConf(true)
    .setAppName("Cassandra")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "localhost")

  val sc = new SparkContext(conf)
  val hdfs = "hdfs://localhost:9000"


  def init(): Unit = {
    val locationConsumer = LocationConsumer.createConsumer()
    LocationConsumer.read(Topic.LocationToCassandra, locationConsumer, new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, Array[Byte]] = locationConsumer.poll(1000)
          for (record <- records) {
            val loc: Location = Location.deserialize(record.value())
            sendLocation(loc)
          }
        }
      }
    })


    val likeConsumer = LikeConsumer.createConsumer()
    LikeConsumer.read(Topic.LikeToCassandra, likeConsumer, new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, Array[Byte]] = likeConsumer.poll(1000)
          for (record <- records) {
            val lk: Like = Like.deserialize(record.value())
            sendLike(lk)
          }
        }
      }
    })

    val postConsumer = PostConsumer.createConsumer()
    PostConsumer.read(Topic.PostsToCassandra, postConsumer, new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, Array[Byte]] = postConsumer.poll(1000)
          for (record <- records) {
            val post: Post = Post.deserialize(record.value())
            sendPost(post)
          }
        }
      }
    })

    val messageConsumer = MessageConsumer.createConsumer()
    MessageConsumer.read(Topic.MessageToCassandra, messageConsumer, new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, Array[Byte]] = messageConsumer.poll(1000)
          for (record <- records) {
            val msg: Message = Message.deserialize(record.value())
            sendMessage(msg)
          }
        }
      }
    })

    val userConsumer = UserConsumer.createConsumer()
    UserConsumer.read(Topic.UserToCassandra, userConsumer, new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, Array[Byte]] = userConsumer.poll(1000)
          for (record <- records) {
            val usr: User = User.deserialize(record.value())
            System.out.println(usr.toString())
            sendProfile(usr)
          }
        }
      }
    })
  }

  def saveToFile(path: String, base: String, table: String): Unit ={
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfs), sc.hadoopConfiguration)
    if(fs.exists(new org.apache.hadoop.fs.Path(path)))
      fs.delete(new org.apache.hadoop.fs.Path(path),true)

    val rdd = sc.cassandraTable(base, table)
    rdd.saveAsTextFile(hdfs + path)
  }

  def saveAllHDFS(base: String): Unit={
    saveToFile("/data/HDFS_user", base, "user")
    saveToFile("/data/HDFS_message", base, "message")
    saveToFile("/data/HDFS_like", base, "like")
    saveToFile("/data/HDFS_location", base, "location")
    saveToFile("/data/HDFS_post", base, "post")
  }

  def readHDFS(path: String): RDD[String] = {
    sc.textFile(path)
  }

  def sendProfile(user: User): Unit ={

    val collection = sc.parallelize(
      Seq(
        (
          Date.from(user.creationTime),
          user.firstName,
          user.lastName,
          user.nickname,
          user.email,
          user.verified)
      )
    )

    collection.saveToCassandra("spark", "user",
      SomeColumns(
        "creation_time",
        "firstname",
        "lastname",
        "nickname",
        "email",
        "verified"
      ),
      writeConf = WriteConf(ifNotExists = true)
    )


    // Check if it has been inserted properly
    val res = sc.cassandraTable("spark", "user")
      .select(
        "creation_time",
        "firstname",
        "lastname",
        "nickname",
        "email",
        "verified"
      )
      .where(
        "creation_time = ? AND firstname = ? AND lastname = ? AND nickname = ? AND email = ? AND verified = ?",
        Date.from(user.creationTime),
        user.firstName,
        user.lastName,
        user.nickname,
        user.email,
        user.verified
      )

    // If the result is correct then send to a new topic for all services who want to know if there is a new user in the db
    if (res.count() == 1) {
      val userProducer: KafkaProducer[String, Array[Byte]] = UserProducer.createProducer()
      UserProducer.send[User](Topic.UserFromCassandra, user, userProducer)
      userProducer.close()
    }
  }
  def sendMessage(msg: Message) : Unit ={
    val collection = sc.parallelize(
      Seq((msg.id.value, Date.from(msg.creationTime), msg.author.value, msg.receiver.value, msg.text)))

    collection.saveToCassandra("spark", "message",
      SomeColumns(
        "id",
        "creation_time",
        "author",
        "receiver",
        "text"
      ),
      writeConf = WriteConf(ifNotExists = true)
    )

    // Check if it has been inserted properly
    val res = sc.cassandraTable("spark", "message")
      .select(
        "id",
        "creation_time",
        "author",
        "receiver",
        "text"
      )
      .where(
        "id = ? AND creation_time = ? AND author = ? AND receiver = ? AND text = ?",
        msg.id.value,
        Date.from(msg.creationTime),
        msg.author,
        msg.receiver,
        msg.text
      )

    // If the result is correct then send to a new topic for all services who want to know if there is a new message in the db
    if (res.count() == 1) {
      val messageProducer: KafkaProducer[String, Array[Byte]] = MessageProducer.createProducer()
      MessageProducer.send[Message](Topic.MessageFromCassandra, msg, messageProducer)
      messageProducer.close()
    }

  }
  def sendLike(like: Like) : Unit ={
    val collection = sc.parallelize(
      Seq((like.id.value, Date.from(like.creationTime), like.author.value, like.postId)))

    collection.saveToCassandra("spark", "like",
      SomeColumns(
        "id",
        "creation_time",
        "author",
        "post_id"
      ),
      writeConf = WriteConf(ifNotExists = true)
    )

    // Check if it has been inserted properly
    val res = sc.cassandraTable("spark", "like")
      .select(
        "id",
        "creation_time",
        "author",
        "post_id"
      )
      .where(
        "id = ? AND creation_time = ? AND author = ? AND post_id = ?",
        like.id.value,
        Date.from(like.creationTime),
        like.author,
        like.postId
      )

    // If the result is correct then send to a new topic for all services who want to know if there is a new like in the db
    if (res.count() == 1) {
      val likeProducer: KafkaProducer[String, Array[Byte]] = LikeProducer.createProducer()
      LikeProducer.send[Like](Topic.LikeFromCassandra, like, likeProducer)
      likeProducer.close()
    }
  }

  def sendLocation(location: Location) : Unit ={
    val collection = sc.parallelize(
      Seq((location.id.value, Date.from(location.creationTime), location.author.value, location.city, location.country)))

    collection.saveToCassandra("spark", "location",
      SomeColumns(
        "id",
        "creation_time",
        "author",
        "city",
        "country"
      ),
      writeConf = WriteConf(ifNotExists = true)
    )

    // Check if it has been inserted properly
    val res = sc.cassandraTable("spark", "location")
      .select(
        "id",
        "creation_time",
        "author",
        "city",
        "country"
      )
      .where(
        "id = ? AND creation_time = ? AND author = ? AND city = ? AND country = ?",
        location.id.value,
        Date.from(location.creationTime),
        location.author,
        location.city,
        location.country
      )

    // If the result is correct then send to a new topic for all services who want to know if there is a new like in the db
    if (res.count() == 1) {
      val locationProducer: KafkaProducer[String, Array[Byte]] = LocationProducer.createProducer()
      LocationProducer.send[Location](Topic.LikeFromCassandra, location, locationProducer)
      locationProducer.close()
    }
  }

  def sendPost(post: Post) : Unit = {
    val collection = sc.parallelize(
      Seq((post.id.value, Date.from(post.creationTime), post.author.value, post.text)))

    collection.saveToCassandra("spark", "post",
      columns = SomeColumns(
        "id",
        "creation_time",
        "author",
        "text"
      ),
      writeConf = WriteConf(ifNotExists = true)
    )

    // Check if it has been inserted properly
    val res = sc.cassandraTable("spark", "post")
      .select(
        "id",
        "creation_time",
        "author",
        "text"
      )
      .where(
        "id = ? AND creation_time = ? AND author = ? AND text = ?",
        post.id.value,
        Date.from(post.creationTime),
        post.author,
        post.text
      )

    // If the result is correct then send to a new topic for all services who want to know if there is a new like in the db
    if (res.count() == 1) {
      val postProducer: KafkaProducer[String, Array[Byte]] = PostProducer.createProducer()
      PostProducer.send[Post](Topic.LikeFromCassandra, post, postProducer)
      postProducer.close()
    }
  }
}
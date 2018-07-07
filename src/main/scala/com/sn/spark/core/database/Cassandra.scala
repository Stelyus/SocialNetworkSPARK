import java.util.Date

import com.datastax.spark.connector._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import com.sn.spark.Topic
import com.sn.spark.core.consumer._
import org.apache.spark._
import com.sn.spark.core.model._
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.collection.JavaConversions._

object Cassandra {

  val conf = new SparkConf(true)
    .setAppName("Cassandra")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "localhost")

  val sc = new SparkContext(conf)

  def init(): Unit = {
    val locationConsumer = LocationConsumer.createConsumer()
    LocationConsumer.read(Topic.LocationToCassandra, locationConsumer, new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, Array[Byte]] = locationConsumer.poll(1000)
          for (record <- records) {
            System.out.println("key: " + record.key())
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
            System.out.println("key: " + record.key())
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
          val records: ConsumerRecords[String, Array[Byte]] = likeConsumer.poll(1000)
          for (record <- records) {
            System.out.println("key: " + record.key())
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
            System.out.println("key: " + record.key())
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
            System.out.println("key: " + record.key())
            val usr: User = User.deserialize(record.value())
            System.out.println(usr.toString())
            //          sendProfile(usr)
          }
        }
      }
    })
  }

  def saveToFile(path: String, base: String, table: String): Unit ={
    val fs=FileSystem.get(Cassandra.sc.hadoopConfiguration)
    if(fs.exists(new Path(path)))
      fs.delete(new Path(path),true)

    val rdd = sc.cassandraTable(base, table)
    rdd.saveAsTextFile(path)
  }

  def readHDFS(path: String): RDD[String] = {
    val lines = sc.textFile(path)
//    lines.collect().foreach(println)
    lines
  }

  def sendProfile(user: User) : Unit ={
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
      )
    )
  }
  def sendMessage(msg: Message) : Unit ={
    val collection = sc.parallelize(
      Seq((msg.id.value, Date.from(msg.creationTime), msg.author, msg.receiver, msg.text)))

    collection.saveToCassandra("spark", "message",
      SomeColumns(
        "id",
        "creation_time",
        "author",
        "receiver",
        "text"
      )
    )
  }
  def sendLike(like: Like) : Unit ={
    val collection = sc.parallelize(
      Seq((like.id.value, Date.from(like.creationTime), like.author, like.postId)))

    collection.saveToCassandra("spark", "like",
      SomeColumns(
        "id",
        "creation_time",
        "author",
        "post_id"
      )
    )
  }
  def sendLocation(location: Location) : Unit ={
    val collection = sc.parallelize(
      Seq((location.id.value, Date.from(location.creationTime), location.author, location.city, location.country)))

    collection.saveToCassandra("spark", "location",
      SomeColumns(
        "id",
        "creation_time",
        "author",
        "city",
        "country"
      )
    )
  }
  def sendPost(post: Post) : Unit ={
    val collection = sc.parallelize(
      Seq((post.id.value, Date.from(post.creationTime), post.author, post.text)))

    collection.saveToCassandra("spark", "post",
      SomeColumns(
        "id",
        "creation_time",
        "author",
        "text"
      )
    )
  }
}

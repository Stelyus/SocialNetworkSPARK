

import java.util.Date

import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.sql.cassandra._
import org.apache.spark._
import com.sn.spark.core.model._
import org.joda.time.DateTime

object Cassandra {
  val conf = new SparkConf(true)
    .setAppName("Cassandra")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "localhost")

  val sc = new SparkContext(conf)

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

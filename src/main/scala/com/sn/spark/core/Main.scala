import java.time.Instant
import com.datastax.spark.connector._
import scala.collection.JavaConversions._
import com.sn.spark.Topic
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Directives
import akka.stream.ActorMaterializer
import com.sn.spark.core.api.routes._
import com.sn.spark.core.model._
import com.sn.spark.core.model.{Id, Message, Post, User}
import org.apache.kafka.clients.producer.KafkaProducer
import com.sn.spark.core.producer.{LikeProducer, LocationProducer, MessageProducer, PostProducer}
import com.sn.spark.core.api.utils.JsonSupport
import com.sn.spark.core.database.{Cassandra, HDFS}
import com.sn.spark.core.database.table._

import scala.concurrent.Future
import com.sn.spark.core.producer._
import java.io._
import java.util.Date

import com.datastax.driver.core.utils.UUIDs
import org.apache.log4j.{Level, Logger}

import scala.io.Source
import scala.util.Random

object Main extends Directives with JsonSupport {
  implicit val system = ActorSystem("my-system")
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    Cassandra.init()
//    HDFS.script()

    val userThread = new Thread {
      override def run() {
        while (true) {
          sendUser()
          Thread.sleep(60000)
        }
      }
    }
//
    val postThread = new Thread {
      override def run() {
        while (true) {
          sendPost()
          Thread.sleep(60000)
        }
      }
    }

    val messageThread = new Thread {
      override def run() {
        while (true) {
          sendMessage()
          Thread.sleep(60000)
        }
      }
    }

    val locationThread = new Thread {
      override def run() {
        while (true) {
          sendLocation()
          Thread.sleep(60000)
        }
      }
    }

    val likeThread = new Thread {
      override def run() {
        while (true) {
          sendLike()
          Thread.sleep(60000)
        }
      }
    }


    userThread.start()
    postThread.start()
    locationThread.start()
    messageThread.start()
    likeThread.start()


    // Init Producer for APIs Routes
    val postProducer = PostProducer.createProducer()
    val messageProducer = PostProducer.createProducer()
    val likeProducer = PostProducer.createProducer()
    val locationProducer = PostProducer.createProducer()
    val userProducer = PostProducer.createProducer()

//    System.out.println("Results:" + UserTable.getById("jojo@gmail.com"))


    Http().bindAndHandle(PostRoutes.getRoute(postProducer, Topic.PostsToCassandra) ~
      MessageRoutes.getRoute(messageProducer, Topic.MessageToCassandra) ~
      LikeRoutes.getRoute(likeProducer, Topic.LikeToCassandra) ~
      LocationRoutes.getRoute(locationProducer, Topic.LocationToCassandra) ~
      UserRoutes.getRoute(userProducer,Topic.UserToCassandra) ~
      SearchRoutes.getRoute(), "localhost", 8080)

    println(s"Server online at http://localhost:8080/")


  }

  def exitProgram(bindingFuture: Future[ServerBinding]): Unit = {
    bindingFuture.flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => {
      system.terminate()
      sys.exit(0)
    })
    println("Exit")
  }

  def sendMessage(): Unit = {

    val messageTopic: String = Topic.MessageToCassandra
    val messageProducer: KafkaProducer[String, Array[Byte]] = MessageProducer.createProducer()
    var i: Int = 0
    // Send 100 Post on Topic posts-topic


    val res = Cassandra.sc.cassandraTable("spark", "user").select("email")

    val r: Random = scala.util.Random
    val userArray = res.collect()
    val userId1 = userArray(r.nextInt(userArray.length))
    val userId2 = userArray(r.nextInt(userArray.length))

    MessageProducer.send[Message](
      messageTopic,
      Message(
        Id[Message](UUIDs.timeBased().toString),
        Instant.now(),
        Id[User](userId1.columnValues(0).toString),
        Id[User](userId2.columnValues(0).toString),
        randomAlpha(5) + ' ' + randomBrand() + ' ' + randomAlpha(5) + ' ' + randomAlpha(5),
        deleted = false
      ),
      messageProducer
    )
    messageProducer.close()
  }

  def sendPost(): Unit = {
    val postTopic: String = Topic.PostsToCassandra

    val postProducer: KafkaProducer[String, Array[Byte]] = PostProducer.createProducer()
    val res = Cassandra.sc.cassandraTable("spark", "user").select("email")

    val r: Random = scala.util.Random
    val userArray = res.collect()
    val userId = userArray(r.nextInt(userArray.length))

    PostProducer.send[Post](
      postTopic,
      Post(
        Id[Post](UUIDs.timeBased().toString),
        Instant.now(),
        Id[User](userId.columnValues(0).toString),
        randomAlpha(5) + ' ' + randomBrand() + ' ' + randomAlpha(5) + ' ' + randomAlpha(5)
      ),
      postProducer
    )

    postProducer.close()
  }

  def sendLike(): Unit = {
    val likesTopic: String = Topic.LikeToCassandra

    val likeProducer: KafkaProducer[String, Array[Byte]] = LikeProducer.createProducer()
    // Send 100 Post on Topic posts-topic

    val userRes = Cassandra.sc.cassandraTable("spark", "user").select("email")
    val postRes = Cassandra.sc.cassandraTable("spark", "post").select("id")

    val r: Random = scala.util.Random
    val userArray = userRes.collect()
    val userId = userArray(r.nextInt(userArray.length))

    val postArray = postRes.collect()
    val postId = postArray(r.nextInt(postArray.length))

    LikeProducer.send[Like](
      likesTopic,
      Like(
        Id[Like](UUIDs.timeBased().toString),
        Id[User](userId.columnValues(0).toString),
        Instant.now(),
        Id[Post](postId.columnValues(0).toString())
      ),
      likeProducer
    )

    likeProducer.close()
  }

  def sendLocation(): Unit = {
    val locTopic: String = Topic.LocationToCassandra

    val locProducer: KafkaProducer[String, Array[Byte]] = LocationProducer.createProducer()
    // Send 100 Post on Topic posts-topic

    val res = Cassandra.sc.cassandraTable("spark", "user").select("email")

    val r: Random = scala.util.Random
    val userArray = res.collect()
    val userId = userArray(r.nextInt(userArray.length))

    LocationProducer.send[Location](
      locTopic,
      Location(
        Id[Location](UUIDs.timeBased().toString),
        Instant.now(),
        Id[User](userId.columnValues(0).toString),
        randomAlpha(5),
        randomAlpha(5)
      ),
      locProducer
    )

    locProducer.close()
  }

  def sendUser(): Unit = {
    val userTopic: String = Topic.UserToCassandra
    val userProducer: KafkaProducer[String, Array[Byte]] = UserProducer.createProducer()
    // Send 100 Post on Topic posts-topic

    val name = randomAlpha(12)
    UserProducer.send[User](
      userTopic,
      User(
        name,
        name,
        name + "@gmail.com",
        name,
        Instant.now(),
        verified = false
      ),
      userProducer
    )
    userProducer.close()
  }



  def randomBrand(): String = {
    val listBrand = List("Nike", "Adidas", "KFC", "Mcdonald", "HM", "BOSS", "Uniqlo", "Burger King", "7 eleven", "NY")
    listBrand(Random.nextInt(listBrand.size))
  }

  // Generate random characters
  def randomAlpha(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z')
    randomStringFromCharList(length, chars)
  }


  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }
}
import java.time.Instant

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
    HDFS.script()

    // Init Producer for APIs Routes
    val postProducer = PostProducer.createProducer()
    val messageProducer = PostProducer.createProducer()
    val likeProducer = PostProducer.createProducer()
    val locationProducer = PostProducer.createProducer()
    val userProducer = PostProducer.createProducer()

    System.out.println("Results:" + UserTable.getById("jojo@gmail.com"))


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


    val filename = "ListOfUserInCassandra"
    val seqUser = Source.fromFile(filename).getLines.toSeq
    val r: Random = scala.util.Random


    while (i < 1000) {
      val user1 = seqUser(r.nextInt(seqUser.length))
      val user2 = seqUser(r.nextInt(seqUser.length))

      MessageProducer.send[Message](
        messageTopic,
        Message(
          Id[Message](UUIDs.timeBased().toString),
          Instant.now(),
          Id[User](user1 + "@gmail.com"),
          Id[User](user2 + "@gmail.com"),
          randomAlpha(5) + ' ' + randomAlpha(5) + ' ' + randomAlpha(5) + ' ' + randomAlpha(5),
          deleted = false
        ),
        messageProducer
      )
      i += 1
    }

    messageProducer.close()
  }

  def sendPost(): Unit = {
    val postTopic: String = Topic.PostsToCassandra

    val postProducer: KafkaProducer[String, Array[Byte]] = PostProducer.createProducer()
    // Send 100 Post on Topic posts-topic


    val filename = "ListOfUserInCassandra"
    for (user <- Source.fromFile(filename).getLines) {
      PostProducer.send[Post](
        postTopic,
        Post(
          Id[Post](UUIDs.timeBased().toString),
          Instant.now(),
          Id[User](user + "@gmail.com"),
          randomAlpha(5) + ' ' + randomAlpha(5) + ' ' + randomAlpha(5) + ' ' + randomAlpha(5)
        ),
        postProducer
      )
    }

    postProducer.close()
  }

  def sendLike(): Unit = {
    val likesTopic: String = Topic.LikeToCassandra

    val likeProducer: KafkaProducer[String, Array[Byte]] = LikeProducer.createProducer()
    var i: Int = 0
    // Send 100 Post on Topic posts-topic

    val filename = "ListOfUserInCassandra"
    val seqUser = Source.fromFile(filename).getLines.toSeq

    val post = "log.bat"
    val seqPost = Source.fromFile(post).getLines.toSeq

    val r: Random = scala.util.Random

    while (i < 100) {
      LikeProducer.send[Like](
        likesTopic,
        Like(
          Id[Like](UUIDs.timeBased().toString),
          Id[User](seqUser(r.nextInt(seqUser.size)) + "@gmail.com"),
          Instant.now(),
          Id[Post](seqPost(r.nextInt(seqPost.size)))
        ),
        likeProducer
      )
      i += 1
    }

    likeProducer.close()
  }

  def sendLocation(): Unit = {
    val locTopic: String = Topic.LocationToCassandra

    val locProducer: KafkaProducer[String, Array[Byte]] = LocationProducer.createProducer()
    var i: Int = 0
    // Send 100 Post on Topic posts-topic

    val filename = "ListOfUserInCassandra"
    val seqUser = Source.fromFile(filename).getLines.toSeq

    val r: Random = scala.util.Random

    while (i < 100) {
      LocationProducer.send[Location](
        locTopic,
        Location(
          Id[Location](UUIDs.timeBased().toString),
          Instant.now(),
          Id[User](seqUser(r.nextInt(seqUser.length)) + "@gmail.com"),
          randomAlpha(5),
          randomAlpha(5)
        ),
        locProducer
      )
      i += 1
    }

    locProducer.close()
  }

  def sendUser(): Unit = {
    val file = new File("ListOfUserInCassandra")
    val bw = new BufferedWriter(new FileWriter(file))

    val userTopic: String = Topic.UserToCassandra
    val userProducer: KafkaProducer[String, Array[Byte]] = UserProducer.createProducer()
    var i: Int = 0
    // Send 100 Post on Topic posts-topic


    while (i < 5000) {

      val name = randomAlpha(12)
      UserProducer.send[User](
        userTopic,
        User(
          name,
          name,
          name + "@gmail.com",
          name,
          Instant.now(),
          false
        ),
        userProducer
      )
      i += 1
      bw.write(name + "\n")
    }

    bw.close()
    userProducer.close()
  }



  // Generate random characters
  def randomAlpha(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z')
    randomStringFromCharList(length, chars)
  }


  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }
}
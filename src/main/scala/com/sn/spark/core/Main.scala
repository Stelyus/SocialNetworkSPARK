import java.time.Instant

import GenerateData.sendPost
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
import org.apache.log4j.{Level, Logger}


object Main extends Directives with JsonSupport {
  implicit val system = ActorSystem("my-system")
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val frozeDataThread = new Thread {
    override def run() {
      while (true) {
        Thread.sleep(1000 * 60 * 60)
        HDFS.saveAllHDFS("spark") 
      }
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    Cassandra.init()
    GenerateData.startThread()

    // Init Producer for APIs Routes
    val postProducer = PostProducer.createProducer()
    val messageProducer = PostProducer.createProducer()
    val likeProducer = PostProducer.createProducer()
    val locationProducer = PostProducer.createProducer()
    val userProducer = PostProducer.createProducer()

    Http().bindAndHandle(PostRoutes.getRoute(postProducer, Topic.PostsToCassandra) ~
      MessageRoutes.getRoute(messageProducer, Topic.MessageToCassandra) ~
      LikeRoutes.getRoute(likeProducer, Topic.LikeToCassandra) ~
      LocationRoutes.getRoute(locationProducer, Topic.LocationToCassandra) ~
      UserRoutes.getRoute(userProducer,Topic.UserToCassandra) ~
      SearchRoutes.getRoute(), "localhost", 8080)

    frozeDataThread.run()

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
}
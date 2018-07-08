package com.sn.spark.core.api.routes
import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import com.datastax.driver.core.utils.UUIDs
import com.sn.spark.core.api.model.Request.PostRequest
import com.sn.spark.core.api.utils.JsonSupport
import com.sn.spark.core.model.{Id, Post, User}
import com.sn.spark.core.producer.PostProducer
import org.apache.kafka.clients.producer.KafkaProducer


// Default ID POST == 1
object PostRoutes extends Directives with JsonSupport {
  def getRoute(producer: KafkaProducer[String, Array[Byte]], str: String) = {
    pathPrefix("api") {
      pathPrefix("post") {
        path("id" / IntNumber) { id =>
          get {
            complete {
              "Received GET request for id " + id
            }
          }
        }
      } ~ {
        post {
          path("post") {
            entity(as[PostRequest]) { postRequest: PostRequest =>
              val id = UUIDs.timeBased().toString

              PostProducer.send[Post](str,
                Post(Id[Post](id), Instant.now() , Id[User](postRequest.author), postRequest.text),
                producer)
              System.out.println(id)
              complete(StatusCodes.Created)
            }
          }
        }
      }
    }
  }
}

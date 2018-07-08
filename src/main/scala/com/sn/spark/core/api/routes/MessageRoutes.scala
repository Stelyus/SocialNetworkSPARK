package com.sn.spark.core.api.routes

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import com.datastax.driver.core.utils.UUIDs
import com.sn.spark.core.api.model.Request.MessageRequest
import com.sn.spark.core.api.utils.JsonSupport
import com.sn.spark.core.model.{Id, Message, User}
import com.sn.spark.core.producer.MessageProducer
import org.apache.kafka.clients.producer.KafkaProducer


object MessageRoutes extends Directives with JsonSupport {
  def getRoute(producer: KafkaProducer[String, Array[Byte]], str: String) = {
    pathPrefix("api") {
      pathPrefix("message") {
        path("id" / IntNumber) { id =>
          get {
            complete {
              "Received GET request for id " + id
            }
          }
        }
      } ~ {
        post {
          path("message") {
            entity(as[MessageRequest]) { msgRequest: MessageRequest =>
              val id = UUIDs.timeBased().toString
              MessageProducer.send[Message](str,
                Message(Id[Message](id), Instant.now(), Id[User](msgRequest.receiver),
                  Id[User](msgRequest.author), msgRequest.text),
                producer)
              System.out.println(msgRequest.toString)
              complete(StatusCodes.Created)
            }
          }
        }
      }
    }
  }

}

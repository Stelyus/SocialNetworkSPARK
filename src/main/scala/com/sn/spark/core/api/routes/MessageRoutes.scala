package com.sn.spark.core.api.routes

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import com.datastax.driver.core.utils.UUIDs
import com.sn.spark.core.api.model.Request.MessageRequest
import com.sn.spark.core.api.model.Response.MessageResponseObject.MessageResponse
import com.sn.spark.core.api.model.Response.PostResponseObject.PostResponse
import com.sn.spark.core.api.routes.PostRoutes.{complete, get, parameter, path}
import com.sn.spark.core.api.utils.JsonSupport
import com.sn.spark.core.database.table.{MessageTable, PostTable}
import com.sn.spark.core.model.{Id, Message, User}
import com.sn.spark.core.producer.MessageProducer
import org.apache.kafka.clients.producer.KafkaProducer


object MessageRoutes extends Directives with JsonSupport {
  def getRoute(producer: KafkaProducer[String, Array[Byte]], str: String) = {
    pathPrefix("api") {
      pathPrefix("message") {
        path("id") {
          get {
            parameter("q") { (m) =>
              System.out.println("query Message Id: " + m)
              val l: MessageResponse = MessageTable.getById(m)
              System.out.println("Result Message: "  + l)
              if (l == null)
                complete(StatusCodes.NotFound)
              else
                complete(l)
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
              System.out.println(id)
              complete(StatusCodes.Created)
            }
          }
        }
      }
    }
  }

}

package com.sn.spark.core.api.routes

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import com.datastax.driver.core.utils.UUIDs
import com.sn.spark.core.api.model.Request.UserRequest
import com.sn.spark.core.api.model.Response.UserResponseObject.UserResponse
import com.sn.spark.core.api.utils.JsonSupport
import com.sn.spark.core.database.table.UserTable
import com.sn.spark.core.model.User
import com.sn.spark.core.producer.UserProducer
import org.apache.kafka.clients.producer.KafkaProducer


object UserRoutes extends Directives with JsonSupport{
  def getRoute(producer: KafkaProducer[String, Array[Byte]], str: String) = {
    pathPrefix("api") {
      pathPrefix("user") {
          get {
            parameter("mail") { (m) =>
              System.out.println("Mail: " + m)
              val l: UserResponse = UserTable.getById(m)
              System.out.println("Result Mail: "  + l);
              if (l == null)
                complete(StatusCodes.NotFound)
              else
                complete(l)
          }
        }
      } ~ {
        post {
          path("user") {
            entity(as[UserRequest]) { userRequest: UserRequest =>
              UserProducer.send[User](str,
                User(userRequest.firstName, userRequest.lastName, userRequest.email, userRequest.nickname,
                  Instant.now()),
                producer)
              System.out.println(userRequest.toString)
              complete(StatusCodes.Created)
            }
          }
        }
      }
    }
  }

}

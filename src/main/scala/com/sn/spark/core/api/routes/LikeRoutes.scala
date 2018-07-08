package com.sn.spark.core.api.routes

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import com.datastax.driver.core.utils.UUIDs
import com.sn.spark.core.api.model.Request.LikeRequest
import com.sn.spark.core.api.model.Response.LikeResponseObject.LikeResponse
import com.sn.spark.core.api.utils.JsonSupport
import com.sn.spark.core.database.table.LikeTable
import com.sn.spark.core.model.{Id, Like, Post, User}
import com.sn.spark.core.producer.LikeProducer
import org.apache.kafka.clients.producer.KafkaProducer


object LikeRoutes extends Directives with JsonSupport {
  def getRoute(producer: KafkaProducer[String, Array[Byte]], str: String) = {
    pathPrefix("api") {
      pathPrefix("like") {
          path("id") {
            get {
              parameter("q") { (m) =>
                System.out.println("Like Id: " + m)
                val l: LikeResponse = LikeTable.getById(m)
                System.out.println("Result Like: "  + l)
                if (l == null)
                  complete(StatusCodes.NotFound)
                else
                  complete(l)
              }
            }
          }
      } ~ {
        post {
          path("like") {
            entity(as[LikeRequest]) { likeRequest: LikeRequest =>
              val id = UUIDs.timeBased().toString
              LikeProducer.send[Like](str,
                Like(Id[Like](id), Id[User](likeRequest.author),  Instant.now() ,Id[Post](String.valueOf(likeRequest.postId))),
                producer)
              System.out.println(likeRequest.toString)
              complete(StatusCodes.Created)
            }
          }
        }
      }
    }
  }
}

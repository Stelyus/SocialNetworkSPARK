package com.sn.spark.core.api.routes
import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import com.datastax.driver.core.utils.UUIDs
import com.sn.spark.core.api.model.Request.PostRequest
import com.sn.spark.core.api.model.Response.PostResponseObject.PostResponse
import com.sn.spark.core.api.utils.JsonSupport
import com.sn.spark.core.database.table.PostTable
import com.sn.spark.core.model.{Id, Post, User}
import com.sn.spark.core.producer.PostProducer
import org.apache.kafka.clients.producer.KafkaProducer


object PostRoutes extends Directives with JsonSupport {
  def getRoute(producer: KafkaProducer[String, Array[Byte]], str: String) = {
    pathPrefix("api") {
      pathPrefix("post") {
        path("id") {
          get {
            parameter("q") { (m) =>
              System.out.println("Post Id: " + m)
              val l: PostResponse = PostTable.getById(m)
              System.out.println("Result Post: "  + l);
              if (l == null)
                complete(StatusCodes.NotFound)
              else
                complete(l)
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

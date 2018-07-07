package com.sn.spark.core.api.routes

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import com.sn.spark.core.api.model.LocationRequest
import com.sn.spark.core.api.utils.JsonSupport
import com.sn.spark.core.model.{Id, Location, User}
import com.sn.spark.core.producer.LocationProducer
import org.apache.kafka.clients.producer.KafkaProducer

object LocationRoutes extends Directives with JsonSupport {
  def getRoute(producer: KafkaProducer[String, Array[Byte]], str: String) = {
    pathPrefix("api") {
      pathPrefix("location") {
        path("id" / IntNumber) { id =>
          get {
            complete {
              "Received GET request for id " + id
            }
          }
        }
      } ~ {
        post {
          path("location") {
            entity(as[LocationRequest]) { locationRequest: LocationRequest =>
              LocationProducer.send[Location](str,
                Location(Id[Location]("1"), Instant.now(), Id[User](locationRequest.author),
                  locationRequest.city, locationRequest.country),
                producer)
              System.out.println(locationRequest.toString)
              complete(StatusCodes.Created)
            }
          }
        }
      }
    }
  }
}

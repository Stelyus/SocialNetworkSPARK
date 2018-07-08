package com.sn.spark.core.api.routes

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import com.datastax.driver.core.utils.UUIDs
import com.sn.spark.core.api.model.Request.LocationRequest
import com.sn.spark.core.api.model.Response.LocationResponseObject.LocationResponse
import com.sn.spark.core.api.utils.JsonSupport
import com.sn.spark.core.database.table.LocationTable
import com.sn.spark.core.model.{Id, Location, User}
import com.sn.spark.core.producer.LocationProducer
import org.apache.kafka.clients.producer.KafkaProducer

object LocationRoutes extends Directives with JsonSupport {
  def getRoute(producer: KafkaProducer[String, Array[Byte]], str: String) = {
    pathPrefix("api") {
      pathPrefix("location") {
        path("id") {
          get {
            parameter("q") { (m) =>
              System.out.println("Location Id: " + m)
              val l: LocationResponse = LocationTable.getById(m)
              System.out.println("Result Location: "  + l)
              if (l == null)
                complete(StatusCodes.NotFound)
              else
                complete(l)
            }
          }
        }
      } ~ {
        post {
          path("location") {
            entity(as[LocationRequest]) { locationRequest: LocationRequest =>
              val id = UUIDs.timeBased().toString
              LocationProducer.send[Location](str,
                Location(Id[Location](id), Instant.now(), Id[User](locationRequest.author),
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

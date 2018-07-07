package com.sn.spark.core.api.routes

import akka.http.scaladsl.model.{HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives
import com.sn.spark.core.api.utils.utils.JsonSupport

object PostRoutes extends Directives with JsonSupport {
  def getRoute() = {
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
            entity(as[HttpEntity]) { entity =>
              System.out.println(entity.getContentType())
              complete(StatusCodes.Created)
            }
          }
        }
      }
    }
  }
}

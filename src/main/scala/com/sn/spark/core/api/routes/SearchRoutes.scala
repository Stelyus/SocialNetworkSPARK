package com.sn.spark.core.api.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import com.sn.spark.core.api.model.Response.SearchResponse
import com.sn.spark.core.api.utils.JsonSupport
import com.sn.spark.core.database.HDFS


object SearchRoutes extends Directives with JsonSupport {
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  def getRoute() = {
    get {
      pathPrefix("api") {
        pathPrefix("search") {
          pathPrefix("date") {
            path("from") {
              get {
                parameter("q", "brand") { (q, brand) =>
                  val l = HDFS.searchAfterDate(format.parse(q), brand)
                  if (l._1.count() == 0 && l._2.count() == 0 )
                    complete(StatusCodes.NotFound)
                  else
                    complete(SearchResponse(l._1.collect().toArray, l._2.collect().toArray))
                }
              }
            } ~ {
              path("to") {
                get {
                  parameter("q", "brand") { (q, brand) =>
                    val l = HDFS.searchBeforeDate(format.parse(q), brand)
                    if (l._1.count() == 0 && l._2.count() == 0 )
                      complete(StatusCodes.NotFound)
                    else
                      complete(SearchResponse(l._1.collect().toArray, l._2.collect().toArray))
                  }
                }
              }
            } ~ {
              path("any") {
                get {
                  parameter("from", "to", "brand") { (from, to, brand) =>
                    val l = HDFS.searchBetweenTwoDate(format.parse(from), format.parse(to), brand)
                    if (l._1.count() == 0 && l._2.count() == 0 )
                      complete(StatusCodes.NotFound)
                    else
                      complete(SearchResponse(l._1.collect().toArray, l._2.collect().toArray))

                  }
                }
              }
            }
          } ~ {
            get {
              parameter("brand") { (brand) =>
                val l = HDFS.searchForever(brand)
                if (l._1.count() == 0 && l._2.count() == 0)
                  complete(StatusCodes.NotFound)
                else
                  complete(SearchResponse(l._1.collect().toArray, l._2.collect().toArray))
              }
             }
            }
          }
        }
      }
    }
}

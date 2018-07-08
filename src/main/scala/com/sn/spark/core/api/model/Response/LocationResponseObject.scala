package com.sn.spark.core.api.model.Response

import com.datastax.spark.connector.CassandraRow
import com.sn.spark.core.api.model.Response.PostResponseObject.PostResponse


object LocationResponseObject {
  final case class LocationResponse(id: String, author: String, city: String, country: String, creation_time: String)

  def toLocationResponse(x: CassandraRow): LocationResponse  = {
    LocationResponse(x.columnValues(x.metaData.namesToIndex.getOrElse("id", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("author", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("city", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("country", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).toString
    )
  }
}
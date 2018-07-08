package com.sn.spark.core.api.model.Response

import com.datastax.spark.connector.CassandraRow


object PostResponseObject {
  final case class PostResponse(id: String, author: String, creation_time: String, text: String)
  def toPostResponse(x: CassandraRow): PostResponse  = {
    PostResponse(x.columnValues(x.metaData.namesToIndex.getOrElse("id", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("author", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("text", 0)).toString)
  }
}
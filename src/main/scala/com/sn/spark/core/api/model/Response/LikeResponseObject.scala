package com.sn.spark.core.api.model.Response

import com.datastax.spark.connector.CassandraRow


object LikeResponseObject {
  final case class LikeResponse(id: String, author: String, creation_time: String, postId: String)
  def toLikeResponse(x: CassandraRow): LikeResponse  = {
    LikeResponse(x.columnValues(x.metaData.namesToIndex.getOrElse("id", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("author", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("postId", 0)).toString)
  }
}
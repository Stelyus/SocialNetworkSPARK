package com.sn.spark.core.api.model.Response


import com.datastax.spark.connector.CassandraRow


object MessageResponseObject {
  final case class MessageResponse(id: String, author: String, creation_time: String, receiver: String, text: String)
  def toMessageResponse(x: CassandraRow): MessageResponse  = {
    MessageResponse(x.columnValues(x.metaData.namesToIndex.getOrElse("id", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("author", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("receiver", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("text", 0)).toString)
  }
}
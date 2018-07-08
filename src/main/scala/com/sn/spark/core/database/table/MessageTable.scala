package com.sn.spark.core.database.table

import com.datastax.spark.connector._
import com.sn.spark.core.api.model.Response.MessageResponseObject._
import com.sn.spark.core.database.{Cassandra, HDFS}
import org.apache.spark.rdd.RDD

object MessageTable {
  val messagePath = "/data/HDFS_message"
  def getById(id: String): MessageResponse  = {
    val rowHDFS: RDD[MessageResponse] = HDFS.readHDFS(HDFS.hdfs + messagePath)
      .map(x => toMessageResponse(x)).filter(x => x.id.equalsIgnoreCase(id))
    if (rowHDFS.count() != 1) {
      val row = Cassandra.sc.cassandraTable("spark", "message")
        .select(
          "id",
          "creation_time",
          "author",
          "receiver",
          "text"
        )
        .where(
          "id = ?",
          id)
      if (row.count() == 1)
        toMessageResponse(row.first())
      else
        null
    } else {
      rowHDFS.first()
    }
  }
}

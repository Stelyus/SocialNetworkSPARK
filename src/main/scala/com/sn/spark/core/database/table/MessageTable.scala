package com.sn.spark.core.database.table

import com.sn.spark.core.api.model.Response.UserResponseObject.{UserResponse, toUserResponse}
import com.sn.spark.core.database.{Cassandra, HDFS}
import org.apache.spark.rdd.RDD

object MessageTable {
/*  val messagePath = "/data/HDFS_message"
  def getById(id: String): MessageResponse  = {
    val rowHDFS: RDD[MessageResponse] = HDFS.readHDFS(HDFS.hdfs + messagePath)
      .map(x => toUserResponse(x)).filter(x => x.id.equalsIgnoreCase(id))
    if (rowHDFS.count() != 1) {
      val row = Cassandra.sc.cassandraTable("spark", "message")
        .select(
          "creation_time",
          "firstname",
          "lastname",
          "nickname",
          "email",
          "verified"
        )
        .where(
          "email = ?",
          email
        )
      if (row.count() == 1)
        toUserResponse(row.first())
      else
        null
    } else {
      rowHDFS.first()
    }
  }*/
}

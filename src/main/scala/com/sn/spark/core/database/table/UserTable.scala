package com.sn.spark.core.database.table

import com.sn.spark.core.api.model.Response.UserResponse
import com.sn.spark.core.database
import com.sn.spark.core.database.Cassandra
import com.sn.spark.core.api.model.Response.UserResponse._
object UserTable extends Table {
  val userPath = "/data/HDFS_user"
  def getById(email: String): UserResponse  = {
    Cassandra.readHDFS(Cassandra.hdfs + userPath)
      .map(x => toUserResponse(x)).filter(x => x.email.equalsIgnoreCase(email)).first()
  }
}

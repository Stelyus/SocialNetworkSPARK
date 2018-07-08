package com.sn.spark.core.api.model.Response

import java.text.SimpleDateFormat
import java.util.Date

import com.datastax.spark.connector.CassandraRow

object UserResponseObject {
  final case class UserResponse(email: String, creation_time: String, fistName: String, lastName: String,
                                nickname: String, verified: Boolean)

  def toUserResponse(x: CassandraRow): UserResponse  = {
    val date: Date = new Date();
    UserResponse(x.columnValues(x.metaData.namesToIndex.getOrElse("email", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("creation_time", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("firstname", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("lastname", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("nickname", 0)).toString,
      x.columnValues(x.metaData.namesToIndex.getOrElse("verified", 0)).toString.toBoolean)
  }
}

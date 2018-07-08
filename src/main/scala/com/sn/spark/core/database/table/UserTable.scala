package com.sn.spark.core.database.table


import com.datastax.spark.connector._
import com.sn.spark.core.database.{Cassandra, HDFS}
import com.sn.spark.core.api.model.Response.UserResponseObject._
import org.apache.spark.rdd.RDD
object UserTable {
  val userPath = "/data/HDFS_user"

  def getById(email: String): UserResponse = {
    val emailCleaned = email.trim()
    val rowHDFS: RDD[UserResponse] = HDFS.readHDFS(HDFS.hdfs + userPath)
      .map(x => toUserResponse(x)).filter(x => x.email.equalsIgnoreCase(emailCleaned))
    if (rowHDFS.count() != 1) {
      val row = Cassandra.sc.cassandraTable("spark", "user")
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
          emailCleaned
        )
      System.out.println(emailCleaned)
      System.out.println("Result Size: " + row.count())
      if (row.count() == 1)
        toUserResponse(row.first())
      else
        null
    } else {
      rowHDFS.first()
    }
  }
}

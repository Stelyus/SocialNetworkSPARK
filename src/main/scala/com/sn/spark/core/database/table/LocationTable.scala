package com.sn.spark.core.database.table


import com.datastax.spark.connector._
import com.sn.spark.core.api.model.Response.LocationResponseObject._
import com.sn.spark.core.database.{Cassandra, HDFS}
import org.apache.spark.rdd.RDD

object LocationTable {
  val locationPath = "/data/HDFS_location"
  def getById(id: String): LocationResponse  = {
    val rowHDFS: RDD[LocationResponse] = HDFS.readHDFS(HDFS.hdfs + locationPath)
      .map(x => toLocationResponse(x)).filter(x => x.id.equalsIgnoreCase(id))
    if (rowHDFS.count() != 1) {
      val row = Cassandra.sc.cassandraTable("spark", "location")
        .select(
          "id",
          "creation_time",
          "author",
          "city",
          "country"
        )
        .where(
          "id = ?",
          id)
      if (row.count() == 1)
        toLocationResponse(row.first())
      else
        null
    } else {
      rowHDFS.first()
    }
  }
}

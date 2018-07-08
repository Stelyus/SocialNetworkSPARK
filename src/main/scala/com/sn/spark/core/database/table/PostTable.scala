package com.sn.spark.core.database.table
import com.datastax.spark.connector._
import com.sn.spark.core.api.model.Response.PostResponseObject._
import com.sn.spark.core.database.{Cassandra, HDFS}
import org.apache.spark.rdd.RDD

object PostTable {
  val postPath = "/data/HDFS_post"
  def getById(id: String): PostResponse = {
    val rowHDFS: RDD[PostResponse] = HDFS.readHDFS(HDFS.hdfs + postPath)
      .map(x => toPostResponse(x)).filter(x => x.id.equalsIgnoreCase(id))
    if (rowHDFS.count() != 1) {
      val row = Cassandra.sc.cassandraTable("spark", "post")
        .select(
          "id",
          "creation_time",
          "author",
          "text"
        )
        .where(
          "id = ?",
          id)
      if (row.count() == 1)
        toPostResponse(row.first())
      else
        null
    } else {
      rowHDFS.first()
    }
  }
}

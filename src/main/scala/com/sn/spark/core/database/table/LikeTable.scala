package com.sn.spark.core.database.table
import com.datastax.spark.connector._
import com.sn.spark.core.api.model.Response.LikeResponseObject._
import com.sn.spark.core.database.{Cassandra, HDFS}
import org.apache.spark.rdd.RDD

object LikeTable {
  val likePath = "/data/HDFS_like"
  def getById(id: String): LikeResponse = {
    val rowHDFS: RDD[LikeResponse] = HDFS.readHDFS(HDFS.hdfs + likePath)
      .map(x => toLikeResponse(x)).filter(x => x.id.equalsIgnoreCase(id))
    if (rowHDFS.count() != 1) {
      val row = Cassandra.sc.cassandraTable("spark", "like")
        .select(
          "id",
          "creation_time",
          "author",
          "post_id"
        )
        .where(
          "id = ? ",
          id)
      if (row.count() == 1)
        toLikeResponse(row.first())
      else
        null
    } else {
      rowHDFS.first()
    }
  }
}

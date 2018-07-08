package com.sn.spark.core.database.table

import org.apache.spark.{SparkConf, SparkContext}

trait Table {
  val conf = new SparkConf(true)
    .setAppName("Cassandra")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "localhost")

  val sc = new SparkContext(conf)

}

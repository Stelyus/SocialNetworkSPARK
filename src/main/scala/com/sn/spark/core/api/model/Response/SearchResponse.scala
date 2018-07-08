package com.sn.spark.core.api.model.Response

import com.sn.spark.core.api.model.Response.MessageResponseObject.MessageResponse
import com.sn.spark.core.api.model.Response.PostResponseObject.PostResponse
import org.apache.spark.rdd.RDD

case class SearchResponse(messages: Array[MessageResponse], posts: Array[PostResponse])

package com.sn.spark.core.api.model.Response

final case class MessageResponse(id: String, author: String, creation_time: String, receiver: String, text: String)
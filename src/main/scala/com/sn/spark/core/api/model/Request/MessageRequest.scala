package com.sn.spark.core.api.model.Request

case class MessageRequest(receiver: String, author: String, text: String)
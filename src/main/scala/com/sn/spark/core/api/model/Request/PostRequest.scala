package com.sn.spark.core.api.model.Request

case class PostRequest(author: String, text: String) {
  override def toString: String = {
    "author: " + author + System.lineSeparator() +
    "text: " + text + System.lineSeparator()
  }
}

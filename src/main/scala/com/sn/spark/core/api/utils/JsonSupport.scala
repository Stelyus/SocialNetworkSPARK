package com.sn.spark.core.api.utils

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import com.sn.spark.core.api.model._


trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val postRequestFormat = jsonFormat2(PostRequest)
  implicit val likeRequestFormat = jsonFormat2(LikeRequest)
  implicit val userRequestFormat = jsonFormat4(UserRequest)
  implicit val messageRequestFormat = jsonFormat3(MessageRequest)
  implicit val locationRequestFormat = jsonFormat3(LocationRequest)
}
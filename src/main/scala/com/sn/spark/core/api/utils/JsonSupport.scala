package com.sn.spark.core.api.utils

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.sn.spark.core.api.model.Request._
import com.sn.spark.core.api.model.Response._
import spray.json.DefaultJsonProtocol


trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  // Request Model To JSON
  implicit val postRequestFormat = jsonFormat2(PostRequest)
  implicit val likeRequestFormat = jsonFormat2(LikeRequest)
  implicit val userRequestFormat = jsonFormat4(UserRequest)
  implicit val messageRequestFormat = jsonFormat3(MessageRequest)
  implicit val locationRequestFormat = jsonFormat3(LocationRequest)

  // Response To JSON
  implicit val postResponseFormat = jsonFormat4(PostResponse)
  implicit val likeResponseFormat = jsonFormat4(LikeResponse)
  implicit val userResponseFormat = jsonFormat6(UserResponse.UserResponse)
  implicit val locationResponseFormat = jsonFormat5(LocationResponse)
  implicit val messageResponseFormat = jsonFormat5(MessageResponse)

}
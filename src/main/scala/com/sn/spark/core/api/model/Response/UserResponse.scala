package com.sn.spark.core.api.model.Response

final case class UserResponse(email: String, creation_time: String, fistName: String, lastName: String,
                        nickname: String, verified: Boolean)

package com.sn.spark.core.model

import java.time.Instant

case class User(id: Id[User], updatedOn: Instant, nickname: String, verified: Boolean, deleted: Boolean)

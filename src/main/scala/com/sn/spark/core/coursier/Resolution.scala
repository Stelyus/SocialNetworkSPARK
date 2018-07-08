package com.sn.spark.core.coursier

import scala.concurrent.ExecutionContext.Implicits.global
import coursier.maven.MavenRepository
import coursier.{Cache, Dependency, Fetch, Module, Resolution}

object ResolutionCoursier {
  val start = Resolution(
    Set(
      Dependency(
        Module("com.fasterxml.jackson.core", "jackson-core"), "2.8.7"
      ),
      Dependency(
        Module("com.fasterxml.jackson.core", "jackson-databind"), "2.8.7"
      ),
      Dependency(
        Module("com.fasterxml.jackson.module", "jackson-module-scala_2.11"), "2.8.7"
      ),
      Dependency(
        Module("datastax", "spark-cassandra-connector"), "2.3.0-s_2.11"
      ),
      Dependency(
        Module("org.apache.spark", "spark-core_2.11"), "2.3.0"
      ),
      Dependency(
        Module("org.apache.spark", "spark-sql_2.11"), "2.3.0"
      ),
      Dependency(
          Module("org.scalatest", "scalatest_2.11"), "2.2.6"
      ),
      Dependency(
        Module("com.typesafe.akka", "akka-http_2.11"), "10.1.1"
      ),
      Dependency(
        Module("com.typesafe.akka", "akka-stream_2.11"), "2.5.11"
      ),
      Dependency(
        Module("com.typesafe.akka", "akka-http-spray-json_2.11"), "10.1.1"
      ),
      Dependency(
        Module("org.apache.kafka", "kafka-streams"), "1.0.0"
      ),
      Dependency(
        Module("org.apache.avro", "avro"), "1.8.2"
      )
    )
  )

  def checkDependencies(): Unit = {
    val repositories = Seq (
      MavenRepository ("https://repo1.maven.org/maven2"),
      MavenRepository("http://central.maven.org/maven2"),
      MavenRepository ("https://dl.bintray.com/spark-packages/maven")
    )

    val fetch = Fetch.from (repositories, Cache.fetch() )

    val resolution = start.process.run (fetch).unsafePerformSync

    val errors: Seq[((Module, String), Seq[String])] = resolution.metadataErrors

    errors.foreach((module) => {
      System.out.println(module._1 + ": " + module._2)
    })
  }
}

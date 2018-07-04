name := "Social Network Spark"

version := "1.0"

scalaVersion := "2.11.8"
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += "datastax" % "spark-cassandra-connector" % "2.3.0-s_2.11"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.4.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.0.0"
libraryDependencies += "org.apache.avro"  %  "avro"  %  "1.8.2"

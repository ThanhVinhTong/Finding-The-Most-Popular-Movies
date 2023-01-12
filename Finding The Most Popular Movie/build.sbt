ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "Finding The Most Popular Movie"
  )

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.31"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.3.1" % "provided"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "4.0.0"
libraryDependencies += "com.codahale.metrics" % "metrics-core" % "3.0.2"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector-driver" % "3.2.0"
libraryDependencies += "joda-time" % "joda-time" % "2.12.2"
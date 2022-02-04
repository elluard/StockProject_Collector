ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

val AkkaVersion = "2.6.14"
val AkkaHttpVersion = "10.2.7"

lazy val root = (project in file("."))
  .settings(
    name := "Collector",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
      "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "3.0.4",
      "org.postgresql" % "postgresql" % "42.2.23",
      "org.slf4j" % "slf4j-simple" % "1.7.35"
    )
  )

name := """bigdata-play-churn"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

resolvers += Resolver.sonatypeRepo("snapshots")

scalaVersion := "2.12.10"

libraryDependencies += guice
libraryDependencies ++= Seq(
  "org.scalatestplus.play" %% "scalatestplus-play" % "3.0.0" % Test,
  "com.google.code.gson" % "gson" % "2.8.1",
  "com.typesafe.play" %% "play-slick" % "3.0.0",
  "com.typesafe.play" %% "play-slick-evolutions" % "3.0.0",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
// Elastic4s
  "com.sksamuel.elastic4s"  %% "elastic4s-core" % "5.4.8",
  "com.sksamuel.elastic4s"  %% "elastic4s-tcp"  % "5.4.8",
  "com.sksamuel.elastic4s"  %% "elastic4s-http" % "5.4.8"
  )
libraryDependencies += "com.h2database" % "h2" % "1.4.194"
libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "0.10.0"

name := """bigdata-play-churn"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

resolvers += Resolver.sonatypeRepo("snapshots")

scalaVersion := "2.11.8"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += guice
libraryDependencies ++= Seq(
  "org.scalatestplus.play" %% "scalatestplus-play" % "3.0.0" % Test,
  "com.google.code.gson" % "gson" % "2.8.1",
  "org.scalaj" % "scalaj-http_2.11" % "2.3.0",
  "org.jsoup" % "jsoup" % "1.8.3",
  "com.h2database" % "h2" % "1.4.194",
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-mllib" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % "2.2.0",
  "org.apache.spark" %% "spark-hive" % "2.2.0",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.3"
)

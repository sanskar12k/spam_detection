ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Fraud detection",
    resolvers += "Apache Spark Releases" at "https://repository.apache.org/content/repositories/releases/"
  )
val sparkVersion = "3.3.2"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.12.18"
libraryDependencies += "com.typesafe.play" % "play_2.12" % "2.8.19"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.6.1" // Keep the modern client
//libraryDependencies += "org.apache.spark" %% "spark-sql-kafka" % sparkVersion // Use the correct connector// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
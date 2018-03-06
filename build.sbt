name := "sparkscala"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.2.0", "org.apache.spark" %% "spark-sql" % "2.2.0", "org.xerial" % "sqlite-jdbc" % "3.16.1","org.apache.spark" % "spark-streaming_2.11" % "2.2.0")
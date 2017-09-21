lazy val commonSettings = Seq(
  organization := "top.wagzhi",
  version := "0.0.1-SNAPSHOT",
  scalaVersion := "2.11.8" ,
  publishTo := Some(Resolver.file("file",  new File( Path.userHome.absolutePath+"/.m2/repository")) ),
  resolvers ++= Seq(
    "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
  )
)

lazy val catrix = (project in file("catrix")).
  settings(commonSettings: _*).
  settings(
    name := "catrix",
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % "2.11.8",
      "com.typesafe.akka" %% "akka-http-core" % "10.0.0" withSources(),
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0" withSources(),
      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
    )
  )

lazy val example = (project in file ("example")).
  settings(commonSettings: _*).
  settings(
    name := "catrix-example",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.1.3"
    )
  ).dependsOn(catrix)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "catrix-root"
  ).aggregate(catrix)
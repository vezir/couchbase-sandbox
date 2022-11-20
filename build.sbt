ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

ThisBuild / organization := "my.organization"

lazy val root = (project in file(".")).
  settings(
    name := "couchbase-spark-project",
    //version := "1.0.0-SNAPSHOT",
    //scalaVersion := "2.12.14",
    assembly / mainClass := Some("Main"),
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1" % "provided,test",
  "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided,test",
  "com.couchbase.client" %% "spark-connector" % "3.2.2"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
name := "project2"
version := "0.1"
scalaVersion := "2.11.12"
//scalaVersion := "2.12.13"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.8"
libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "3.1.2",
//   "org.apache.spark" %% "spark-sql" % "3.1.2",
//  "org.apache.spark" %% "spark-hive" % "3.1.2",
  "org.jsoup" % "jsoup" % "1.8.3",
  "org.apache.hadoop" % "hadoop-aws" % "2.6.5",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.375"
//  "org.apache.hadoop" % "hadoop-aws" % "2.6.5"
)
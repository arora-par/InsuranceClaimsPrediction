organization := "assignment"
name := "Insurance-Claims-Final-Project"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.6"

val scalaTestVersion = "2.2.4"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

//EclipseKeys.withSource := true

libraryDependencies ++= Seq(
	"org.spire-math" %% "spire" % "0.10.1",
	"org.apache.commons" % "commons-math3" % "3.6",
	"org.scalatest" %% "scalatest" % scalaTestVersion % "test",
	
	// spark core
	  "org.apache.spark" %% "spark-core" % "1.6.1",
	  "org.apache.spark" %% "spark-sql" % "1.6.1",
	  "org.apache.spark" %% "spark-mllib" % "1.6.1",
	// spark packages
	  "com.databricks" %% "spark-csv" % "1.4.0"
)


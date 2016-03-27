organization := "assignment"
name := "Insurance-Claims-Final-Project"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.7"

val scalaTestVersion = "2.2.4"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

//EclipseKeys.withSource := true

libraryDependencies ++= Seq(
	"org.scala-lang.modules" %% "scala-xml" % "1.0.4",
	"org.spire-math" %% "spire" % "0.10.1",
	"org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
	"org.apache.commons" % "commons-math3" % "3.6",
	"org.scalatest" %% "scalatest" % scalaTestVersion % "test",
	
	// spark core
	  "org.apache.spark" %% "spark-core" % "1.6.1"
	  //"org.apache.spark" %% "spark-mllib_2.10" % "1.5.1",
	  // spark packages
	  //"com.databricks" %% "spark-csv_2.10" % "1.2.0"
)


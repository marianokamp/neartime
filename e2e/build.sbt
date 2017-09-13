name := "e2e"

version := "1.0"

scalaVersion := "2.11.8"

//resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += DefaultMavenRepository
//resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies ++= {

  val sparkV = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-streaming" % sparkV,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkV,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV)
}
//libraryDependencies += "junit" % "junit" % "4.10" % "test"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"
//libraryDependencies += "com.databricks" %% "spark-avro" % "3.2.0"

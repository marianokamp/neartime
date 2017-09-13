name := "hbase"

version := "1.0"

scalaVersion := "2.11.8"


resolvers += DefaultMavenRepository
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies +=  "org.apache.hbase" % "hbase-client" % "1.2.0"
libraryDependencies +=  "org.apache.hbase" % "hbase-common" % "1.2.0"

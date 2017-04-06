name := "hbase"

version := "1.0"

scalaVersion := "2.11.8"

//resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"


/* resolvers ++= Seq(
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases/",
  "Cloudera repo" at "//repository.cloudera.com/artifactory/cloudera-repos/"
)*/
resolvers += DefaultMavenRepository
//libraryDependencies += "zhzhan" % "shc" % "0.0.11-1.6.1-s_2.10"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
//libraryDependencies += "org.apache.hbase" %% "hbase-spark" % "2.0.0-SNAPSHOT"
libraryDependencies +=  "org.apache.hbase" % "hbase-client" % "1.2.0"
libraryDependencies +=  "org.apache.hbase" % "hbase-common" % "1.2.0"




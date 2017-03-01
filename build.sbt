name := "SparkTasks"

version := "1.0"

scalaVersion := "2.11.3"


val sparkVersion = "2.0.1"

resolvers += "mapr" at "http://repository.mapr.com/maven/"

libraryDependencies ++= Seq(
  //"org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" % "spark-core_2.11" % "2.0.1",
"org.apache.spark" % "spark-sql_2.11" % "2.0.1",
  //"org.apache.spark" %% "spark-sql" % sparkVersion,
  //"org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.hbase" % "hbase-client" % "1.1.1-mapr-1602-m7-5.2.0",
  "org.apache.hbase" % "hbase-common" % "1.1.1-mapr-1602-m7-5.2.0",
  "org.apache.hbase" % "hbase-server" % "1.1.1-mapr-1602-m7-5.2.0" excludeAll ExclusionRule(organization = "org.mortbay.jetty")

)
name := "spark-avro-parquet-read-write-kite"

version := "1.0"

scalaVersion := "2.10.5"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8", // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture"
)

val sparkVersion = "1.2.0-cdh5.3.2"

val hadoopVersion = "2.5.0-cdh5.3.2"

val sparkAvroVersion = "0.2.0"

val avroVersion = "1.7.6-cdh5.3.2"

val parquetAvroVersion = "1.5.0-cdh5.3.2"

val kiteVersion = "0.15.0-cdh5.3.2"

val scalaTestVersion = "2.2.4"

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.kitesdk" % "kite-data-core" % kiteVersion % "compile",
  "org.kitesdk" % "kite-data-mapreduce" % kiteVersion % "compile",
  "org.apache.avro" % "avro" % avroVersion % "compile" exclude("org.mortbay.jetty", "servlet-api") exclude("io.netty", "netty") exclude("org.apache.avro", "avro-ipc") exclude("org.mortbay.jetty", "jetty"),
  "org.apache.avro" % "avro-mapred" % avroVersion % "compile" classifier("hadoop2") exclude("org.mortbay.jetty", "servlet-api") exclude("io.netty", "netty") exclude("org.apache.avro", "avro-ipc") exclude("org.mortbay.jetty", "jetty"),
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "compile" excludeAll ExclusionRule("javax.servlet"),
  "org.scalatest" % "scalatest_2.10" % scalaTestVersion % "test"
)

parallelExecution in Test := false
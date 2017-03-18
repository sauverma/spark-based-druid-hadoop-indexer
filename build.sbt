name := "RawStreamBatchEnricher"
version := "0.1"
scalaVersion := "2.10.6"
fork := true

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.7" 
libraryDependencies += "com.couchbase.client" % "spark-connector_2.10" % "1.2.0" 
libraryDependencies += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.5.0"
libraryDependencies += "joda-time" % "joda-time" % "2.9.6"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.2"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.0"

resolvers += "OSS Sonatype" at "https://repo1.maven.org/maven2/"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}

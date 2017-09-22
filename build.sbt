mainClass in (Compile, packageBin) := Some("com.kunyan.bigv.Scheduler")

name := "bigv"

version := "1.0"

scalaVersion := "2.10.4"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"

libraryDependencies += "org.ansj" % "ansj_seg" % "0.9"

libraryDependencies += "org.jsoup" % "jsoup" % "1.8.3"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.38"

libraryDependencies += "com.ibm.icu" % "icu4j" % "56.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2" % "provided"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2" exclude("org.apache.spark", "spark-streaming_2.10")

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "redis.clients" % "jedis" % "2.8.0"

libraryDependencies += "org.json" % "json" % "20090211"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

unmanagedJars in Compile += file("D:/jar/LabelMapping-assembly-1.0(8).jar")

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("javax", "el", xs@_*) => MergeStrategy.last
  case PathList("com", "gilt", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("org", "junit", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case PathList("javax", "xml", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("org", "codehaus", xs@_*) => MergeStrategy.last
  case PathList("org", "xerial", xs@_*) => MergeStrategy.last
  case PathList("com", "sun", "xml",xs@_*) => MergeStrategy.last
  case PathList("com", "sun", "istack",xs@_*) => MergeStrategy.last
  case PathList("win32", xs@_*) => MergeStrategy.last
  case PathList("org", "slf4j", xs@_*) => MergeStrategy.last
  case PathList("org", "objectweb", xs@_*) => MergeStrategy.last
  case PathList("net", "jpountz", xs@_*) => MergeStrategy.last
  case PathList("linux", xs@_*) => MergeStrategy.last
  case PathList("io", "netty", xs@_*) => MergeStrategy.last
  case PathList("org", "xerial", xs@_*) => MergeStrategy.last
  case PathList("META-INF","native", xs@_*) => MergeStrategy.last
  case PathList("darwin",xs@_*) => MergeStrategy.last
  case PathList(ps @ _*) if ps.last endsWith "versions.properties" => MergeStrategy.first
  case PathList("com", "thoughtworks", xs@_*) => MergeStrategy.last
  case PathList("com", "fasterxml", xs@_*) => MergeStrategy.last
  case PathList("ch", xs@_*) => MergeStrategy.last
  case PathList("junit", xs@_*) => MergeStrategy.last
  case PathList("scala", xs@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "compiler.properties" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


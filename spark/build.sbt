name := "SparkMicropathing"

version := "1.0"

scalaVersion := "2.9.3"

retrieveManaged := true

mainClass in (Compile,run) := Some("org.xdata.analytics.spark.micropathing.Main")

libraryDependencies ++= Seq(
  "org.ow2.asm" % "asm" % "4.0",
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.5.0",
  "org.apache.spark" %% "spark-core" % "0.8.0-incubating",
  "org.eclipse.jetty.orbit" % "javax.servlet" % "2.5.0.v201103041518",
  "joda-time" % "joda-time" % "2.1",
  "org.joda" % "joda-convert" % "1.2",
  "org.apache.hbase" % "hbase" % "0.94.6-cdh4.5.0",
  "org.apache.avro" % "avro" % "1.7.4",
  "com.oculusinfo" % "geometric-utilities" % "0.1.1",
  "com.oculusinfo" % "math-utilities" % "0.1.1",
  "com.oculusinfo" % "tile-generation" % "0.1.1",
  "com.oculusinfo" % "binning-utilities" % "0.1.1"
)


resolvers += {
   val r = new org.apache.ivy.plugins.resolver.IBiblioResolver
   r.setM2compatible(true)
   r.setName("Maven2 Main")
   r.setRoot("http://repo1.maven.org/maven2/")
   r.setCheckconsistency(false)
   new RawRepository(r)
}


resolvers ++= Seq(
   "Local Maven Repository" at "file:///"+Path.userHome+"/.m2/repository",
   "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
   "CDH 4.1.2" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
   "Spray Repository" at "http://repo.spray.cc/",
   "Jetty Eclipse" at "http://repo1.maven.org/maven2/"
)


ivyXML :=
<dependency org="org.eclipse.jetty.orbit" name="javax.servlet" rev="3.0.0.v201112011016">
<artifact name="javax.servlet" type="orbit" ext="jar"/>
</dependency>


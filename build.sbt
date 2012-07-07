organization := "st.happy_camper.hadoop.yarn"

name := "yarn-applications"

version := "0.0.1-SNAPSHOT"

description := "YARN application examples"

homepage := Some(url("http://ueshin.github.com/yarn-applications"))

startYear := Some(2012)

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

organizationName := "Happy-Camper Street"

organizationHomepage := Some(url("http://happy-camper.st"))

resolvers += "Cloudera Releases" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.0.1" excludeAll(
    ExclusionRule("com.sun.jdmk", "jmxtools"),
    ExclusionRule("com.sun.jmx", "jmxri"),
    ExclusionRule("javax.jms", "jms")
  ),
  "org.apache.hadoop" % "hadoop-yarn-common" % "2.0.0-cdh4.0.1" excludeAll(
    ExclusionRule("com.sun.jdmk", "jmxtools"),
    ExclusionRule("com.sun.jmx", "jmxri"),
    ExclusionRule("javax.jms", "jms")
  )
)

// for tests
libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "1.9" % "test",
  "junit" % "junit" % "4.8.2" % "test")

testOptions in Test += Tests.Argument("console", "junitxml")

EclipseKeys.withSource := true

name         := "info602-final-project"
organization := "vcu"
version      := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.13"

val sparkVersion = "3.1.1"

resolvers ++= Seq(
  Resolver.mavenCentral,
  "Spark Packages" at "https://repos.spark-packages.org/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"   % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.scalatest"    %% "scalatest"   % "3.2.9"      % Test
)

// Include "provided" deps on the classpath when running locally with sbt run
run in Compile := Defaults.runTask(
  fullClasspath in Compile,
  mainClass in (Compile, run),
  runner in (Compile, run)
).evaluated

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings"
)

fork in run := false

// sbt-assembly: exclude Scala stdlib so the cluster provides it
assembly / assemblyOption ~= {
  _.withIncludeScala(false)
}

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*)             => MergeStrategy.discard
  case "reference.conf"                          => MergeStrategy.concat
  case "application.conf"                        => MergeStrategy.concat
  case x if x.endsWith(".proto")                => MergeStrategy.first
  case _                                         => MergeStrategy.first
}

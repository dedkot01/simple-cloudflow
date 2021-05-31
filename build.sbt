import sbt.Keys._
import sbt._

lazy val root =
  Project(id = "root", base = file("."))
    .settings(
      name := "root",
      skip in publish := true
    )
    .withId("root")
    .settings(commonSettings)
    .aggregate(
      datamodel,
      dataIngress
    )

lazy val pipeline = appModule("pipeline")
  .enablePlugins(CloudflowApplicationPlugin)
  .settings(commonSettings)

lazy val datamodel = appModule("datamodel")
  .enablePlugins(CloudflowLibraryPlugin)

lazy val dataIngress = appModule("data-ingress")
  .enablePlugins(CloudflowAkkaPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "com.lightbend.akka" %% "akka-stream-alpakka-file" % "2.0.2",
      "com.lightbend.akka" %% "akka-stream-alpakka-csv"  % "2.0.2"
    )
  )
  .dependsOn(datamodel)

lazy val validateSubscriptionData = appModule("validate-subscription-data")
  .enablePlugins(CloudflowAkkaPlugin)
  .dependsOn(datamodel)

lazy val aggregation = appModule("aggregation")
  .enablePlugins(CloudflowFlinkPlugin)
  .dependsOn(datamodel)

lazy val dataStore = appModule("data-store")
  .enablePlugins(CloudflowSparkPlugin)
  .dependsOn(datamodel)

lazy val statusCollector = appModule("status-collector")
  .enablePlugins(CloudflowFlinkPlugin)
  .dependsOn(datamodel)

def appModule(moduleID: String): Project = {
  Project(id = moduleID, base = file(moduleID))
    .settings(
      name := moduleID
    )
    .withId(moduleID)
    .settings(commonSettings)
}

lazy val commonSettings = Seq(
  organization := "org.dedkot",
  scalaVersion := "2.12.11",
  javacOptions += "-Xlint:deprecation",
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-target:jvm-1.8",
    "-Xlog-reflective-calls",
    "-Xlint",
    "-Ywarn-unused",
    "-Ywarn-unused-import",
    "-deprecation",
    "-feature",
    "-language:_",
    "-unchecked"
  ),
  scalacOptions in (Compile, console) --= Seq("-Ywarn-unused", "-Ywarn-unused-import"),
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value
)

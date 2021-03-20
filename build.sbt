ThisBuild / scalaVersion := "2.13.5"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.riskdient"
ThisBuild / organizationName := "DBZIO example"

lazy val root = (project in file("."))
  .settings(
    name := "dbzio",
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick" % "3.3.3",
      "com.typesafe.slick" %% "slick-hikaricp" % "3.3.3",
      "org.postgresql" % "postgresql" % "9.4-1206-jdbc42",
      "com.chuusai" %% "shapeless" % "2.3.3",
      "org.slf4j" % "slf4j-nop" % "1.7.26",
      "dev.zio" %% "zio" % "1.0.5",
      "dev.zio" %% "zio-test" % "1.0.5" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Test / fork := true
  )

lazy val scala213 = "2.13.5"
lazy val scala212 = "2.12.14"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala213, scala212, scala211)

val zioVersion = "1.0.10"
val catsVersion = "2.6.1"
val catsVersion211 = "2.0.0"
val scalaCheckVersion = "1.15.2"

val slickVersion = "3.3.3"

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.riskdient"
ThisBuild / organizationName := "DBZIO"

lazy val root = (project in file("."))
  .settings(
    name := "dbzio",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, 11)) => Seq(
          "org.typelevel" %% "cats-core" % catsVersion211 % Test,
          "org.typelevel" %% "cats-laws" % catsVersion211 % Test
        )
        case _ => Seq(
          "org.typelevel" %% "cats-core" % catsVersion % Test,
          "org.typelevel" %% "cats-laws" % catsVersion % Test
        )
      }
    } ++ Seq(
      "com.typesafe.slick" %% "slick" % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp" % slickVersion % Test,
      "com.h2database" % "h2" % "1.4.200" % Test,
      "com.chuusai" %% "shapeless" % "2.3.3",
      "org.slf4j" % "slf4j-nop" % "1.7.26" % Test,
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % Test,
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.3" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Test / fork := true
  )

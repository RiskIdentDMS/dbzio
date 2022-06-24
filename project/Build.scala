import sbt.Keys._
import sbt._
import scalafix.sbt.ScalafixPlugin
import scalafix.sbt.ScalafixPlugin.autoImport._

object Build {

  lazy val Version = new {
    val h2             = "2.1.210"
    val slf4j          = "1.7.36"
    val zio            = "1.0.13"
    val cats           = "2.7.0"
    val scalaCheck     = "1.15.4"
    val slick          = "3.3.3"
    val shapeless      = "2.3.8"
    val shapelessCheck = "1.3.0"

    lazy val scala213 = List(
      "2.13.8",
      "2.13.7",
      "2.13.6"
    )
    lazy val scala212 =
      List("2.12.15", "2.12.14")
  }

  lazy val supportedScalaVersions = Version.scala213 ++ Version.scala212
  def createScalacOptions(version: String, unusedImport: Boolean): List[String] = {
    val base = List(
      "-explaintypes",
      "-feature",
      "-Xlint",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-deprecation",
      "-language:higherKinds",
      "-opt-inline-from:com.riskident.**",
      "-opt:l:method,inline",
      "-opt-warnings:none"
    )

    val wConf = List(
      "-Ywarn-macros:after",
      s"-Wconf:" + List(
        "cat=deprecation:ws",
        "cat=feature:ws",
        "cat=unused-params:s",
        "cat=unused-pat-vars:e",
        "cat=unused-privates:s",
        "cat=unused-locals:s",
        "cat=unused-nowarn:s",
        "src=src_managed/.*:s",
        s"cat=unused-imports:${if (unusedImport) "e" else "s"}"
      ).mkString(",")
    )

    CrossVersion.partialVersion(version) match {
      case Some((2, 12)) if version.endsWith(".15") => base ++ wConf :+ "-Ypartial-unification"
      case Some((2, 13))                            => base ++ wConf
      case _                                        => base
    }
  }
  lazy val commonSettings = Seq(
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick"        % Version.slick,
      "com.chuusai"        %% "shapeless"    % Version.shapeless,
      "dev.zio"            %% "zio"          % Version.zio,
      "org.scala-lang"     % "scala-reflect" % scalaVersion.value
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Test / fork := true,
    scalacOptions := createScalacOptions(scalaVersion.value, true),
    Compile / console / scalacOptions := createScalacOptions(scalaVersion.value, false),
    Test / console / scalacOptions := (Compile / console / scalacOptions).value
  )

  implicit class ProjectOps(val project: Project) extends AnyVal {
    def withScalafix: Project =
      project
        .enablePlugins(ScalafixPlugin)
        .settings(
          addCompilerPlugin("org.scalameta" % "semanticdb-scalac" % "4.5.4" cross CrossVersion.full),
          semanticdbEnabled := true, // enable SemanticDB
          semanticdbVersion := scalafixSemanticdb.revision,
          scalacOptions += "-Yrangepos",
          scalafixOnCompile := true
        )

    def withCommonSettings: Project =
      project
        .settings(commonSettings: _*)

  }
}

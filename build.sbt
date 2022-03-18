lazy val supportedScalaVersions = List(Version.scala213, Version.scala212)

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.riskdient"
ThisBuild / organizationName := "DBZIO"

def createScalacOptions(version: String, unusedImport: Boolean): List[String] = {
  val base = List(
    "-explaintypes",
    "-feature",
    "-Xlint",
    "-Ywarn-macros:after",
    "-Xlint",
    "-unchecked",
    "-encoding",
    "UTF-8",
    "-feature",
    "-deprecation",
    "-Wconf:" + List(
      "cat=deprecation:ws",
      "cat=feature:ws",
      "cat=unused-params:s",
      "cat=unused-pat-vars:s",
      "cat=unused-privates:s",
      "cat=unused-locals:s",
      "cat=unused-nowarn:s",
      "src=src_managed/.*:s",
      "cat=unused-imports&src=target/.*:s",
      "cat=unused-imports&src=views/.*:s",
      s"cat=unused-imports:${if (unusedImport) "e" else "s"}"
    ).mkString(",")
  )

  CrossVersion.partialVersion(version) match {
    case Some((2, 12)) => base :+ "-Ypartial-unification"
    case _             => base
  }
}

lazy val root = (project in file("."))
  .settings(
    name := "dbzio",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      "com.typesafe.slick"         %% "slick"                     % Version.slick,
      "com.chuusai"                %% "shapeless"                 % Version.shapeless,
      "dev.zio"                    %% "zio"                       % Version.zio,
      "com.h2database"             % "h2"                         % Version.h2 % Test,
      "org.slf4j"                  % "slf4j-nop"                  % Version.slf4j % Test,
      "com.typesafe.slick"         %% "slick-hikaricp"            % Version.slick % Test,
      "dev.zio"                    %% "zio-test"                  % Version.zio % Test,
      "dev.zio"                    %% "zio-test-sbt"              % Version.zio % Test,
      "org.scalacheck"             %% "scalacheck"                % Version.scalaCheck % Test,
      "org.typelevel"              %% "cats-core"                 % Version.cats % Test,
      "org.typelevel"              %% "cats-laws"                 % Version.cats % Test,
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.15" % Version.shapelessCheck % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Test / fork := true,
    scalacOptions := createScalacOptions(scalaVersion.value, true),
    Compile / console / scalacOptions := createScalacOptions(scalaVersion.value, false),
    Test / console / scalacOptions := (Compile / console / scalacOptions).value
  )

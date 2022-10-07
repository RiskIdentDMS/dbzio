import ReleaseTransformations._
import Build._

ThisBuild / name := "dbzio"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / organization := "io.github.RiskIdentDMS"
ThisBuild / organizationName := "Risk.Ident GmbH"
ThisBuild / homepage := Some(url("https://github.com/RiskIdentDMS/dbzio"))
ThisBuild / organizationHomepage := Some(url("https://github.com/RiskIdentDMS"))
ThisBuild / description := "Monadic bridge between ZIO and DBIO"
ThisBuild / licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"

lazy val dbzio = (project in file("dbzio")).withScalafix.withCommonSettings
  .settings(
    name := "dbzio"
  )

lazy val test = (project in file("test")).withScalafix.withCommonSettings
  .dependsOn(dbzio)
  .settings(
    name := "dbzio-test",
    libraryDependencies ++= Seq(
      "com.h2database"     % "h2"              % Version.h2,
      "com.typesafe.slick" %% "slick-hikaricp" % Version.slick,
      "dev.zio"            %% "zio-test"       % Version.zio
    )
  )

lazy val tests = (project in file("tests"))
  .dependsOn(dbzio, test)
  .withScalafix
  .withCommonSettings
  .settings(
    publish / skip := true,
    libraryDependencies ++= Seq(
      "com.h2database"             % "h2"                         % Version.h2             % Test,
      "org.slf4j"                  % "slf4j-nop"                  % Version.slf4j          % Test,
      "com.typesafe.slick"         %% "slick-hikaricp"            % Version.slick          % Test,
      "dev.zio"                    %% "zio-test"                  % Version.zio            % Test,
      "dev.zio"                    %% "zio-test-sbt"              % Version.zio            % Test,
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.15" % Version.shapelessCheck % Test,
      "org.scalacheck"             %% "scalacheck"                % Version.scalaCheck     % Test,
      "org.typelevel"              %% "cats-core"                 % Version.cats           % Test,
      "org.typelevel"              %% "cats-laws"                 % Version.cats           % Test
    )
  )

lazy val root = (project in file("."))
  .aggregate(dbzio, test, tests)
  .withScalafix
  .settings(
    publish / skip := true,
    pgpKeyRing := Some(file("~/.gnupg/pubring.kbx")),
    crossScalaVersions := supportedScalaVersions,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    /**
      * release settings
      */
    publishMavenStyle := true,
    // Workaround from https://www.scala-sbt.org/1.x/docs/Cross-Build.html#Note+about+sbt-release
    // don't use sbt-release's cross facility
    releaseCrossBuild := false,
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      //releaseStepCommandAndRemaining("+test"),
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      releaseStepCommandAndRemaining("+publishSigned"),
      releaseStepCommand("sonatypeBundleRelease"),
      setNextVersion,
      commitNextVersion,
      pushChanges
    ),
    releaseVcsSign := true,
    resolvers ++= Seq(Resolver.mavenLocal, Resolver.sonatypeRepo("staging"))
  )

// Remove all additional repository other than Maven Central from POM
pomIncludeRepository := { _ => false }

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

// For all Sonatype accounts created on or after February 2021
sonatypeCredentialHost := "s01.oss.sonatype.org"

sonatypeProfileName := organization.value

publishTo := sonatypePublishToBundle.value

scmInfo := Some(ScmInfo(url("https://github.com/RiskIdentDMS/dbzio"), "git@github.com:RiskIdentDMS/dbzio.git"))

developers := List(
  Developer(
    id = "SuperIzya",
    name = "Ilya Kazovsky",
    email = "gkazovsky@gmail.com",
    url = url("https://github.com/SuperIzya/")
  )
)

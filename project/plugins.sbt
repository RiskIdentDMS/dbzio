resolvers += Resolver.sonatypeRepo("staging")

addSbtPlugin("org.jetbrains.scala" % "sbt-ide-settings" % "1.1.1")
addSbtPlugin("org.scalameta"       % "sbt-scalafmt"     % "2.4.6")
addSbtPlugin("com.github.sbt"      % "sbt-release"      % "1.1.0")
addSbtPlugin("ch.epfl.scala"       % "sbt-scalafix"     % "0.9.31")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.13")
addSbtPlugin("com.github.sbt" % "sbt-release"  % "1.1.0")
addSbtPlugin("com.github.sbt" % "sbt-pgp"      % "2.1.2")

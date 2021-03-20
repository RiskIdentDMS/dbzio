#!/usr/bin/env amm

import $ivy.`dev.zio::zio:1.0.5`, zio._, zio.console._
import sys.process._

val cmdBase = Seq("docker-compose")
val startCmd = Seq("up", "-d")
val stopCmd = Seq("down", "-v", "--remove-orphans")
val start = Process(cmdBase ++ startCmd)
val stop = Process(cmdBase ++ stopCmd)

@main
def main(args: String*): Unit = {

  val printError: Throwable => URIO[Console, Unit] = {
    case e: RuntimeException =>
      val msg = e.getMessage
      putStrLn(s"Error: $msg").unless(msg.isEmpty)
    case e => putStrLn(s"Unexpected error: $e")
  }

  val action = for {
    _ <- ZIO.effect(start.!)
    _ <- putStrLn("Dockers started. To stop them press Enter twice")
    _ <- getStrLn.repeatN(1)
    _ <- ZIO.effect(stop.!)
  } yield ()

  Runtime.default.unsafeRun(action.foldM(
    printError,
    _ => ZIO.unit
  ).provideLayer(Console.live))
}
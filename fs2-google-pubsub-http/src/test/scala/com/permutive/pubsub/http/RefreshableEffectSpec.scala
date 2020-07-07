package com.permutive.pubsub.http

import cats.effect.IO
import com.permutive.pubsub.http.util.RefreshableEffect
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RefreshableEffectSpec extends AnyFlatSpec with Matchers {
  behavior.of("RefreshableEffect")

  implicit val ctx   = IO.contextShift(global)
  implicit val timer = IO.timer(global)

  it should "retry when eff failed" in {
    var omg = 0
    val eff = IO {
      omg = omg + 1
      if (omg > 3)
        ()
      else
        throw new Error(s"Not omg yet ${omg}")
    }

    RefreshableEffect
      .createRetryResource(
        eff,
        1.second,
        IO(println("Refresh ok")),
        {
          case err => IO(println(s"refresh error ${err}"))
        },
        1.second,
        identity,
        Int.MaxValue,
        {
          case err => IO(println(s"No more retry error ${err}"))
        },
      )
      .use { refresh =>
        refresh.value
      }
      .unsafeRunSync()
  }
}

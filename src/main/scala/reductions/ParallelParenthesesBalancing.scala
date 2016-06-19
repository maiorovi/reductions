package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def iterate(chars:Array[Char], i:Int, acc:Int):Boolean = if (chars.isEmpty || i >= chars.length ) {
      acc == 0
    }  else {
      if ( acc < 0 ) {
        false
      } else {
        chars(i) match {
          case '(' => iterate(chars, i + 1, acc + 1)
          case ')' => iterate(chars, i + 1, acc - 1)
          case _ => iterate(chars, i + 1, acc)
        }
      }
    }

    iterate(chars, 0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(from: Int, until: Int, arg1: Int, arg2: Int):(Int,Int) /*: ???*/ = {
      var i = from
      var acc1 = arg1
      var min = arg2
      while ( i < until) {

        chars(i) match {
          case '(' => {
            acc1 += 1
            min = Math.min(min, acc1)
          }
          case ')' => {
            acc1 -= 1
            min = Math.min(min, acc1)
          }
          case _ => {}
        }

        i += 1
      }

      (acc1,min)
    }

    def reduce(from: Int, until: Int):List[(Int,Int)] /*: ???*/ = {
      if (until - from <= threshold) {
        List(traverse(from, until, 0, 0))
      } else {
        val mid = from + (until - from) / 2
        val (r1, r2) = parallel(reduce(from, mid), reduce(mid, until))
        r1 ++ r2
      }
    }
  var balance = 0

    for ((x, min) <- reduce(0, chars.length)) {
      if (balance + min < 0) {
        return false
      } else {
        balance += x
      }
    }

    balance == 0
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}

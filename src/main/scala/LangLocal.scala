package edu.cmu.spf.iris

import scala.math._

/** Language for local representation models. */
object LangLocal {

  case class Exp() {

  }

  case class Cond(guard: Exp, iftrue: Exp, iffalse: Exp) extends Exp() {

  }

  case class Const(constval: Double) extends Exp() {

  }

  case class Var(id: String) extends Exp() {

  }

  case class Unary(
    op: Operation.Unary,
    input: Exp
  ) extends Exp() {

  }

  case class Binary(
    op: Operation.Binary,
    left: Exp,
    right: Exp
  ) extends Exp() {

  }

}

object Operation {
  trait Unary[I,O] {
    val sym: String
    val imp: I => O
  }

  trait Binary[I,O] {
    val sym: String
    val imp: I => I => O
  }

  object Arithmetic {
    object Unary {

    }

    object Binary {
     def add[E <: Numeric]: Binary[E,E] = new Binary[E,E] {
        val sym = "+"
        val imp = e1 => e2 => e1 + e2
      }
      def mul[E <: Numeric]: Binary[E,E] = new Monoid[E,E] {
        val sym = "*"
        val imp = e1 => e2 => e1 * e2
      }
      def and[E <: Boolean]: Binary[E,E] = new Binary[E,E] {
        val sym = "∧"
        val imp = e1 => e2 => e1 && e2
      }
      def or[E <: Boolean]: Binary[E,E] = new Binary[E,E] {
        val sym = "∨"
        val imp = e1 => e2 => e1 || e2
      }
      def xor[E <: Boolean]: Binary[E,E] = new Binary[E,E] {
        val sym = "⊕"
        val imp = e1 => e2 => e1 ^ e2
      }
    }
  }
}
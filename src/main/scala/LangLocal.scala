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

  case class Associative(
    op: Operation:Associative,
    elements: Seq[Exp]
  ) extends Exp() {

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
  trait Monoid[E] {
    val sym: String
    val zero: E
    val plus: E => E => E
  }

  trait Unary[I,O] {
    val sym: String
    val fun: I => O
  }

  trait Binary[I,O] {
    val sym: String
    val fun: I => I => O
  }

  object Arithmetic {
    object Unary {

    }

    object Binary {

    }

    object Monoid {
      def add[E <: Numeric]: Monoid[E] = new Monoid[E] {
        val sym = "+"
        val zero = zero
        val plus = e1 => e2 => e1 + e2
      }
      def mul[E <: Numeric]: Monoid[E] = new Monoid[E] {
        val sym = "*"
        val zero = one
        val plus = e1 => e2 => e1 * e2
      }
      def and[E <: Boolean]: Monoid[E] = new Monoid[E] {
        val sym = "∧"
        val zero = true
        val plus = e1 => e2 => e1 && e2
      }
      def or[E <: Boolean]: Monoid[E] = new Monoid[E] {
        val sym = "∨"
        val zero = false
        val plus = e1 => e2 => e1 || e2
      }
      def xor[E <: Boolean]: Monoid[E] = new Monoid[E] {
        val sym = "⊕"
        val zero = false
        val plus = e1 => e2 => e1 ^ e2
      }
    }
  }
}
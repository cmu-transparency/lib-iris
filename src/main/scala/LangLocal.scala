package edu.cmu.spf.iris

import scala.math._

/*

/** Language for local representation models. */
object LangLocal {
  type DT = Double
  object Expression {

    trait Exp
    case class Cond(
      guard: Exp,
      iftrue: Exp,
      iffalse: Exp) extends Exp {
      override def toString: String =
        "ite(" + Seq(guard, iftrue, iffalse).map(_.toString).mkString(".") + ")"
    }

    case class Const(constval: DT) extends Exp {

    }

    case class Var(id: String) extends Exp {

    }

    case class Unary(
      op: Operation.Unary,
      input: Exp) extends Exp {

    }

    case class Binary(
      op: Operation.Binary,
      left: Exp,
      right: Exp) extends Exp {

    }
  }

  object Operation {

    trait Unary {
      val sym: String
      def imp: DT => DT
    }

    trait Binary {
      val sym: String
      def imp: DT => DT => DT
    }

    object Arithmetic {
      def add[E](implicit num: Numeric[E]): Binary = new Binary {
        val sym = "+"
        def imp(e1: E, e2: E): E = num.plus(e1, e2)
      }
      def mul[E](implicit num: Numeric[E]): Binary = new Binary {
        val sym = "*"
        def imp(e1: E, e2: E): E = num.times(e1, e2)
      }
      /*      def and[E](implicit num: Numeric[E]): Binary[E,E] = new Binary[E,E] {
        val sym = "∧"
        val imp = e1 => e2 => num.(e1, e2)
      }
      def or[E](implicit num: Numeric[E]): Binary[E,E] = new Binary[E,E] {
        val sym = "∨"
        val imp = e1 => e2 => num.()e1 || e2)
      }
      def xor[E](implicit num: Numeric[E]): Binary[E,E] = new Binary[E,E] {
        val sym = "⊕"
        val imp = e1 => e2 => num.()e1 ^ e2)
      }*/
    }
  }
}

 */ 
package edu.cmu.spf.iris

import scala.math._

/** Language for local representation models. */
object LangLocal {

  trait Exp[T]

  case class Cond[T](
    guard: Exp[T],
    iftrue: Exp[T],
    iffalse: Exp[T]
  ) extends Exp[T] {

  }

  case class Const[T](constval: T) extends Exp[T] {

  }

  case class Var[T](id: String) extends Exp[T] {

  }

  case class Unary[T](
    op: Operation.Unary[T,T],
    input: Exp[T]
  ) extends Exp[T] {

  }

  case class Binary[T](
    op: Operation.Binary[T,T],
    left: Exp[T],
    right: Exp[T]
  ) extends Exp[T] {

  }

}

object Operation {
  trait Unary[I,O] {
    val sym: String
    def imp: I => O
  }

  trait Binary[I,O] {
    val sym: String
    def imp: I => I => O
  }

  object Arithmetic {
    object Unary {

    }

    object Binary {
     def add[E](implicit num: Numeric[E]): Binary[E,E] = new Binary[E,E] {
        val sym = "+"
        def imp(e1:E, e2:E): E = num.plus(e1,e2)
      }
      def mul[E](implicit num: Numeric[E]): Binary[E,E] = new Binary[E,E] {
        val sym = "*"
        def imp(e1:E, e2:E): E = num.times(e1, e2)
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
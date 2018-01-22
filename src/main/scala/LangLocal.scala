package edu.cmu.spf.iris

import scala.math._

object Operation {
  trait Unary[E] {
    val sym: String
    def imp: E => E
  }

  trait Binary[E] {
    val sym: String
    def imp: E => E => E
  }

  object Arithmetic {
    def add[E](implicit num: Numeric[E]): Binary[E] = new Binary[E] {
      val sym = "+"
      def imp: E => E => E = e1 => e2 => num.plus(e1, e2)
    }
    def mul[E](implicit num: Numeric[E]): Binary[E] = new Binary[E] {
      val sym = "*"
      def imp: E => E => E = e1 => e2 => num.times(e1, e2)
    }
    /*
    def and[E](implicit num: Numeric[E]): Binary[E,E] = new Binary[E,E] {
      val sym = "∧"
      val imp = e1 => e2 => num.(e1, e2)
    }
    def or[E](implicit num: Numeric[E]): Binary[E,E] = new Binary[E,E] {
      val sym = "∨"
      val imp = e1 => e2 => num.(e1 || e2)
    }
    def xor[E](implicit num: Numeric[E]): Binary[E,E] = new Binary[E,E] {
     val sym = "⊕"
     val imp = e1 => e2 => num.(e1 ^ e2)
     }
     */
  }
}

/*

case class Lens[A,B](val get: A => B, val set: A => B => A)

trait Exp[T <: Exp[T]] {
  val lenses: Iterable[Lens[T,Exp[T]]]
}

/*
 * Language for local representation models. */
object LangLocal {
  import Operation._

  type DT = Double

  abstract class DoubleExpression extends Exp[DoubleExpression]
  type DExp = DoubleExpression

  object Expression {
    case class Cond(
      val guard: DExp,
      val iftrue: DExp,
      val iffalse: DExp) extends DExp {

      val lenses: Iterable[Lens[Cond,DExp]] =
        Seq(Cond.guard, Cond.iftrue, Cond.iffalse)

      override def toString: String =
        "ite(" + Seq(guard, iftrue, iffalse).map(_.toString).mkString(".") + ")"
    }

    object Cond {
      val guard = Lens[Cond, DExp](
        _.guard,
        exp => new_guard => Cond(new_guard, exp.iftrue, exp.iffalse)
      )

      val iftrue = Lens[Cond, DExp](
        _.iftrue,
        exp => new_iftrue => Cond(exp.guard, new_iftrue, exp.iffalse)
      )

      val iffalse = Lens[Cond, DExp](
        _.iffalse,
        exp => new_iffalse => Cond(exp.guard, exp.iftrue, new_iffalse)
      )
    }

    case class Const(constval: DT) extends DExp {
      override def toString:String = constval.toString
    }

    case class Var(id: String) extends DExp {
      override def toString: String = id.toString
    }

    case class Unary(
      op: Operation.Unary[DT],
      input: DExp) extends DExp {
      override def toString: String =
        "(" + op.toString + " " + input.toString + ")"
    }

    case class Binary(
      op: Operation.Binary[DT],
      left: DExp,
      right: DExp) extends DExp {
      override def toString: String =
        "(" + left.toString + " " + op.toString + " " + right.toString + ")"
    }
  }
}

 */
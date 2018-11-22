package edu.cmu.spf.iris

import scala.math._

object Information {

  def surprise(p: Double): Double = -log(p) / log(2.0)

  def entropy(p: Double): Double = p * surprise(p) + (1.0 - p) * surprise(1.0 - p)

}

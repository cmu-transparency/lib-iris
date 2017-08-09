package edu.cmu.spf.iris

object StringUtils {
  def tab(s: Object): String = {
    "  " + s.toString.split("\n").mkString("  \n")
  }
}
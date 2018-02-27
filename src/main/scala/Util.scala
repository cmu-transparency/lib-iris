package edu.cmu.spf.iris

object StringUtils {
  def tab(s: Object): String = {
    "  " + s.toString.split("\n").mkString("  \n")
  }
}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path

object FSUtils {
  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  def deleteIfExists(p: Path): Unit = {
    if (fs.exists(p)) {
      println(s"WARNING: $p exists, deleting")
      fs.delete(p)
    }
  }
}

object DUtils {
  def guessDelimeter(f: java.io.File): String = {
    val src = scala.io.Source.fromFile(f)
    val line = src.getLines.take(1).next
    src.close

    val contains_comma = line.contains(",")
    val contains_tab = line.contains("\t")

    (contains_comma, contains_tab) match {
      case (true, false) => ","
      case (_, true) => "\t"
      case _ =>
        println(s"WARNING: could not guess delimeter of $src")
        ","
    }
  }

}
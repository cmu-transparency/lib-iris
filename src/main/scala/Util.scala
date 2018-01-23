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
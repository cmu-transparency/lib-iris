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

object MUtils {
  val log2 = Math.log(2.0)

  def lg(a: Double): Double = Math.log(a) / log2
}

object IUtils {
  import org.apache.spark.sql.{DataFrame, Column}
  import MUtils.{lg}
  import collection.parallel.mutable.ParArray

  object Contingency {
    def normalized_mutual_information(
      cont: ParArray[ParArray[Long]]
    ): Double = {
      val a = cont.transpose
      val count_x = a.map{_.sum}
      val count_y = cont.map{_.sum}
      val ent_x = Count.entropy(count_x)
      val ent_y = Count.entropy(count_y)
      val ent_x_g_y = conditional_entropy(cont)
      val maxent = math.max(ent_x, ent_y)
      if (maxent == 0.0) {
        return 0.0
      } else {
        (ent_x - ent_x_g_y) / maxent
      }
    }

    def mutual_information(
      cont: ParArray[ParArray[Long]]
    ): Double = {
      val a = cont.transpose
      val count_x = a.map{_.sum}
      val ent_x = Count.entropy(count_x)
      val ent_x_g_y = conditional_entropy(cont)
      ent_x - ent_x_g_y
    }

    def conditional_entropy(
      cont: ParArray[ParArray[Long]]
    ): Double = {
      val conds = cont.map {count => (count.sum, Count.entropy(count))}
      val total = conds.map{_._1}.sum.toDouble
      conds.map{case (cnt, ent) => (cnt.toDouble / total) * ent}.sum
    }
  }

  object Count {
    def entropy(count: ParArray[Long]): Double = {
      val total: Double = count.sum
      - count.map{c =>
        val p = (c:Double) / total
        if (p == 0) { 0.0 } else { p * lg(p) }
      }.sum
    }
  }

  def normalized_mutual_information(
    df: DataFrame, c1: Column, c2: Column
  ): Double = {
    Contingency.normalized_mutual_information(
      df.stat.crosstab(c1.toString, c2.toString)
        .collect.map{r => r.toSeq.tail.asInstanceOf[Seq[Long]].toArray.par}.par
    )
  }

  def mutual_information(
    df: DataFrame, c1: Column, c2: Column
  ): Double = {
    Contingency.mutual_information(
      df.stat.crosstab(c1.toString, c2.toString)
        .collect.map{r => r.toSeq.tail.asInstanceOf[Seq[Long]].toArray.par}.par
    )

  }

}
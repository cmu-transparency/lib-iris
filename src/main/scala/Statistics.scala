package edu.cmu.spf.iris

import scala.collection.JavaConversions._

import org.apache.log4j.{ Logger, Level }

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SQLContext }

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql.types._

import java.io.{ File, FileWriter, BufferedWriter }

object Statistics extends App {
  case class CmdLine(
    input_file: File = new File("."),
    discrete_bins: Int = 10)

  val parser = new scopt.OptionParser[CmdLine]("Statistics") {
    head("Statistics", "0.1")
    opt[File]('i', "in")
      .required
      .action((x, c) => c.copy(input_file = x))
    opt[Int]('b', "bins")
      .action((x, c) => c.copy(discrete_bins = x))
  }
  parser.parse(args, CmdLine()) match {
    case Some(cmdline) => RunStatistics(cmdline)
    case None => ()
  }

  def RunStatistics(cmdline: CmdLine): Unit = {
    val data = SparkUtil.csvReader.option("delimiter", "\t")
      .load(cmdline.input_file.toString)

    val quantizer = new QuantileDiscretizer()
      .setNumBuckets(cmdline.discrete_bins)

    data.columns
      .zip(data.schema.fields)
      .foreach {
        case (column_name, f) =>
          val column_type = f.dataType
          println(s"$column_name ($column_type)")

          val column = {
            column_type match {
              case StringType | IntegerType => data.select(column_name)
              case DoubleType =>
                quantizer
                  .setInputCol(column_name)
                  .setOutputCol(column_name + "_discrete")
                  .fit(data).transform(data)
                  .drop(column_name)
                  .withColumnRenamed(column_name + "_discrete", column_name)
                  .select(column_name + "_discrete")
            }
          }

          column
            .groupBy(column_name)
            .count
            .collect.foreach {
              case row =>
                println(s"  ${row.get(0)}: ${row.get(1)}")
            }
      }
  }
}
